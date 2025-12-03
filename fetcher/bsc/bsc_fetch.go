package bsc

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cosmos/gogoproto/proto"
	"github.com/ethereum/go-ethereum/common"
	oracletypes "github.com/imua-xyz/imuachain/x/oracle/types"
	"github.com/imua-xyz/price-feeder/fetcher/types"
	fetchertypes "github.com/imua-xyz/price-feeder/fetcher/types"
	feedertypes "github.com/imua-xyz/price-feeder/types"
)

const batchSize = 100

var (
	finalizedBlock           uint64
	finalizedVersion         uint64
	finalizedWithdrawVersion uint64

	latestChangesBytes = fetchertypes.NSTZeroChanges
)

func (s *source) fetch(token string) (*fetchertypes.PriceInfo, error) {
	if fetchertypes.NSTToken(token) != fetchertypes.NativeTokenBSC {
		return nil, feedertypes.ErrTokenNotSupported.Wrap(fmt.Sprintf("only support native-eth-restaking %s, got:%s", fetchertypes.NativeTokenBSC, token))
	}

	// Round block height down to nearest 100 for consistency with batch querying
	height := s.getCurrentHeight() / 100 * 100
	sInfos, v, wV := s.Stakers.GetStakersNoCopy()
	if height <= finalizedBlock || v <= finalizedVersion || wV <= finalizedWithdrawVersion {
		s.Logger().Info("fetch delegators from beaconchain, no change in height(round to 100) or version, return latestChangesBytes", "height", height, "version", finalizedVersion, "withdrawVersion", finalizedWithdrawVersion)
		return &types.PriceInfo{
			Price: string(latestChangesBytes),
			// combine height and versions as roundID in priceInfo
			RoundID: fmt.Sprintf("%s|%s|%s", strconv.FormatUint(finalizedBlock, 10), strconv.FormatUint(finalizedVersion, 10), strconv.FormatUint(finalizedWithdrawVersion, 10)),
		}, nil
		//		return nil, nil
	}

	// TODO: 20 should be set as a value that less than fetching interval
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	delegators := []common.Address{}
	delegator2Idx := make(map[common.Address]uint32)
	for stakerIdx, sInfo := range sInfos {
		// this should not happen
		if len(sInfo.Validators) != 1 {
			s.Logger().Error("for bsc native restaker, one and only one valiators must be bonded to the staker")
			continue
		}
		addr := common.HexToAddress(sInfo.Validators[0])
		delegators = append(delegators, addr)
		delegator2Idx[addr] = stakerIdx
	}
	grouped := make(map[common.Address][]common.Address)
	jobs := make([]job, 0)
	for _, d := range delegators {
		if v, exists := s.cache.get(d); exists {
			grouped[v] = append(grouped[v], d)
		} else {
			jobs = append(jobs, job{
				kind:       jobKindFullScan,
				delegators: []common.Address{d},
				block:      height,
			})
		}
	}
	for v, ds := range grouped {
		l := len(ds)
		if l == 0 {
			continue
		}
		aIdx := 0
		for aIdx < l {
			bIdx := aIdx + batchSize
			if bIdx > l {
				bIdx = l
			}
			jobs = append(jobs, job{
				kind:       jobKindBatchDelegators,
				credit:     v,
				delegators: ds[aIdx:bIdx],
				block:      height,
			})
			aIdx += batchSize
		}
	}
	res, err := s.pool.runBatch(ctx, jobs)
	if err != nil {
		return nil, err
	}
	changedStakerBalances := make([]*oracletypes.NSTKV, 0, len(sInfos))
	for _, d := range delegators {
		sIdx := delegator2Idx[d]
		sInfo := sInfos[sIdx]
		if sInfo.Balance != res[d] || sInfo.WithdrawVersion > finalizedWithdrawVersion {
			changedStakerBalances = append(changedStakerBalances, &oracletypes.NSTKV{
				StakerIndex: sIdx,
				Balance:     res[d],
			})
		}
	}

	if len(changedStakerBalances) > 0 {
		s.Logger().Info("fetched delegator amounts from bsc, some amounts of delegators have changed")
		sort.Slice(changedStakerBalances, func(i, j int) bool {
			return changedStakerBalances[i].StakerIndex < changedStakerBalances[j].StakerIndex
		})
		nstBC := oracletypes.RawDataNST{
			Version:           v,
			WithdrawVersion:   wV,
			NstBalanceChanges: changedStakerBalances,
		}
		bz, err := proto.Marshal(&nstBC)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal nstBalanceChanges, error:%w", err)
		}
		latestChangesBytes = bz
	} else {
		s.Logger().Info("fetched delegator amounts from bsc, all amounts remain unchanged")
		latestChangesBytes = fetchertypes.NSTZeroChanges
	}

	finalizedBlock = height
	finalizedVersion = v
	finalizedWithdrawVersion = wV

	return &types.PriceInfo{
		Price:   string(latestChangesBytes),
		RoundID: fmt.Sprintf("%s|%s|%s", strconv.FormatUint(finalizedBlock, 10), strconv.FormatUint(v, 10), strconv.FormatUint(wV, 10)),
	}, nil
}

func (s *source) reload(config, token string) error {
	return nil
}
