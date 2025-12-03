package bsc

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	nsttypes "github.com/imua-xyz/price-feeder/fetcher/nst/types"
	fetchertypes "github.com/imua-xyz/price-feeder/fetcher/types"
	feedertypes "github.com/imua-xyz/price-feeder/types"
)

const (
	envConf = "oracle_env_bsc.yaml"

	multicallABIJSON = `[
	{
	  "inputs": [
		{
		  "components": [
			{"internalType": "address","name": "target","type": "address"},
			{"internalType": "bytes","name": "callData","type": "bytes"}
		  ],
		  "internalType": "struct Call[]",
		  "name": "calls",
		  "type": "tuple[]"
		}
	  ],
	  "name": "aggregate",
	  "outputs": [
		{"internalType": "uint256","name": "blockNumber","type": "uint256"},
		{"internalType": "bytes[]","name": "returnData","type": "bytes[]"}
	  ],
	  "stateMutability": "nonpayable",
	  "type": "function"
	}
  ]`

	stakeCreditABIJSONPooled = `[
  {
    "inputs": [
      {"internalType": "address","name": "account","type": "address"}
    ],
    "name": "getPooledBNB",
    "outputs": [
      {"internalType": "uint256","name": "", "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`
	stakeCreditABIJSONLocked = `[
  {
    "inputs": [
      {"internalType": "address","name": "delegator","type": "address"},
      {"internalType": "uint256","name": "number","type": "uint256"}
    ],
    "name": "lockedBNBs",
    "outputs": [
      {"internalType": "uint256","name": "", "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`

	stakeHubABIJSON = `[
  {
    "inputs": [
      {"internalType": "uint256","name": "offset","type": "uint256"},
      {"internalType": "uint256","name": "limit","type": "uint256"}
    ],
    "name": "getValidators",
    "outputs": [
      {"internalType": "address[]","name": "operatorAddrs","type": "address[]"},
      {"internalType": "address[]","name": "creditAddrs","type": "address[]"},
      {"internalType": "uint256","name": "totalLength","type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`
)

var (
	logger        feedertypes.LoggerInf
	defaultSource *source

	errNoValidatorFound = errors.New("delegator not bonded to any validator")
	errInvalidJob       = errors.New("invalid worker job")

	// errBig is expected to be used as a helper, and should be constructed where needed,
	// since d and amt are not in scope here. Remove it from global scope.
)

type config struct {
	MuilticallAddr string        `yaml:"multicall_addr"`
	StakeHubAddr   string        `yaml:"stake_hub_addr"`
	CacheTTL       time.Duration `yaml:"cache_ttl"`
	URLs           struct {
		Bsc string `yaml:"bsc"`
	} `yaml:"urls"`
	// NumWorkers     int `yaml:"num_workers"`
	// JobBuffer      int `yaml:"job_buffer"`
}

type validatorInfo struct {
	Operator       common.Address
	CreditContract common.Address
}

type cacheEntry struct {
	ValidatorCredit common.Address // credit contract address
	// ExpireAt        time.Time
}

type validatorSet struct {
	mu   sync.RWMutex
	vals []validatorInfo
}

type delegatorCache struct {
	// TODO: add cap, and clear expired entries when hit cap
	//	ttl   time.Duration
	store sync.Map
}

type source struct {
	*nsttypes.Source
	client *ethclient.Client

	multicallAddr common.Address
	stakeHubAddr  common.Address

	// ttl time.Duration

	multicallABI abi.ABI
	pooledABI    abi.ABI
	lockedABI    abi.ABI
	stakeHubABI  abi.ABI

	//	mu    sync.RWMutex
	// cache map[common.Address]cacheEntry
	cache *delegatorCache
	//	validators []validatorInfo // operator + creditContract
	validators *validatorSet

	pool *workerPool
}

func (d *delegatorCache) set(addr, v common.Address) {
	d.store.Store(addr, cacheEntry{
		ValidatorCredit: v,
		//	ExpireAt:        time.Now().Add(d.ttl),
	})
}

func (d *delegatorCache) get(delegator common.Address) (common.Address, bool) {
	v, ok := d.store.Load(delegator)
	if !ok {
		return common.Address{}, false
	}
	entry := v.(cacheEntry)
	// if time.Now().After(entry.ExpireAt) || (entry.ValidatorCredit == common.Address{}) {
	if entry.ValidatorCredit == (common.Address{}) {
		return common.Address{}, false
	}
	return entry.ValidatorCredit, true
}

func (d *delegatorCache) delete(addr common.Address) {
	d.store.Delete(addr)
}

// func newDelegatorCache(ttl time.Duration) *delegatorCache {
func newDelegatorCache() *delegatorCache {
	//	if ttl <= 0 {
	//		ttl = 24 * time.Hour
	//	}
	return &delegatorCache{
		//	ttl:   ttl,
		store: sync.Map{},
	}
}

func (vs *validatorSet) getValidators() []validatorInfo {
	vs.mu.RLock()
	cpy := make([]validatorInfo, len(vs.vals))
	copy(cpy, vs.vals)
	vs.mu.RUnlock()
	return cpy
}

func (vs *validatorSet) setValidators(vals []validatorInfo) {
	vs.mu.Lock()
	vs.vals = make([]validatorInfo, len(vals))
	copy(vs.vals, vals)
	vs.mu.Unlock()
}
func newValidatorSet() *validatorSet {
	return &validatorSet{
		mu:   sync.RWMutex{},
		vals: make([]validatorInfo, 0, 100),
	}
}

func init() {
	fetchertypes.SourceInitializers[fetchertypes.Bsc] = initBSC
}

func initBSC(cfgPath string, l feedertypes.LoggerInf) (fetchertypes.SourceInf, error) {
	if logger = l; logger == nil {
		if logger = feedertypes.GetLogger("fetcher_bsc"); logger == nil {
			return nil, feedertypes.ErrInitFail.Wrap("logger is not initialized")
		}
	}

	cfg, err := nsttypes.ParseConfig[config](cfgPath, envConf)
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to parse config, error:%v", err))
	}

	multicallABI, err := abi.JSON(strings.NewReader(multicallABIJSON))
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to parse multicall ABI, error:%v", err))
	}

	pooledABI, err := abi.JSON(strings.NewReader(stakeCreditABIJSONPooled))
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to parse stake credit ABI, error:%v", err))
	}

	lockedABI, err := abi.JSON(strings.NewReader(stakeCreditABIJSONLocked))
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to parse stake credit ABI, error:%v", err))
	}

	stakeHubABI, err := abi.JSON(strings.NewReader(stakeHubABIJSON))
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to parse stake hub ABI, error:%v", err))
	}

	client, err := ethclient.Dial(cfg.URLs.Bsc)
	if err != nil {
		return nil, feedertypes.ErrInitFail.Wrap(fmt.Sprintf("failed to dial BSC, error:%v", err))
	}

	defaultSource = &source{}

	*defaultSource = source{
		Source:        nsttypes.NewSource(logger, fetchertypes.Bsc, defaultSource.fetch, cfgPath, defaultSource.reload),
		client:        client,
		multicallAddr: common.HexToAddress(cfg.MuilticallAddr),
		stakeHubAddr:  common.HexToAddress(cfg.StakeHubAddr),
		multicallABI:  multicallABI,
		pooledABI:     pooledABI,
		lockedABI:     lockedABI,
		stakeHubABI:   stakeHubABI,
		cache:         newDelegatorCache(),
		validators:    newValidatorSet(),
		pool: startNewWorkerPool(defaultSource, workerPoolConfig{
			numWorkers: 10,
			jobBuffer:  100,
		}),
	}

	return defaultSource, nil
}
