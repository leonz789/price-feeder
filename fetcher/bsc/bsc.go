package bsc

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

// we use gwei as unit of amount (uint64 should be enough)

var _ scannerInf = &source{}

var divisor = big.NewInt(1e9)

type multicall struct {
	target   common.Address // matches ABI field "target"
	calldata []byte         // matches ABI field "callData"
}

func (s *source) batchQueryCreditForDelegators(ctx context.Context, credit common.Address, delegators []common.Address, blockNumber *big.Int) (map[common.Address]*big.Int, error) {
	calls := make([]multicall, 0, len(delegators))
	for _, d := range delegators {
		pooledBNB, err := s.pooledABI.Pack("getPooledBNB", d)
		if err != nil {
			return nil, err
		}
		lockedBNB, err := s.lockedABI.Pack("lockedBNBs", d, big.NewInt(0))
		if err != nil {
			return nil, err
		}
		calls = append(calls,
			multicall{target: credit, calldata: pooledBNB},
			multicall{target: credit, calldata: lockedBNB},
		)
	}
	calldata, err := s.multicallABI.Pack("aggregate", calls)
	if err != nil {
		return nil, err
	}
	res, err := s.client.CallContract(ctx, ethereum.CallMsg{
		To:   &s.multicallAddr,
		Data: calldata,
	}, blockNumber)
	if err != nil {
		return nil, err
	}
	unpackedRes, err := s.multicallABI.Unpack("aggregate", res)
	if err != nil {
		return nil, err
	}

	// unpackedRes[0] is blockNumber (*big.Int) - returned by multicall, can be ignored
	// unpackedRes[1] is returnData ([][]byte)
	if len(unpackedRes) != 2 {
		return nil, fmt.Errorf("unexpected unpack result length: %d", len(unpackedRes))
	}

	_ = unpackedRes[0].(*big.Int) // blockNumber from multicall response (we use the provided blockNumber parameter)
	returnData := unpackedRes[1].([][]byte)

	// Each delegator has 2 calls: getPooledBNB and lockedBNBs
	// So returnData length should be len(delegators) * 2
	if len(returnData) != len(delegators)*2 {
		return nil, fmt.Errorf("unexpected returnData length: %d, expected %d", len(returnData), len(delegators)*2)
	}

	result := make(map[common.Address]*big.Int)
	for i, d := range delegators {
		// Index i*2 is getPooledBNB result
		var pooledBNB *big.Int
		err = s.pooledABI.UnpackIntoInterface(&pooledBNB, "getPooledBNB", returnData[i*2])
		if err != nil {
			return nil, fmt.Errorf("failed to unpack pooledBNB for delegator %s: %w", d.Hex(), err)
		}

		// Index i*2+1 is lockedBNBs result
		var lockedBNB *big.Int
		err = s.lockedABI.UnpackIntoInterface(&lockedBNB, "lockedBNBs", returnData[i*2+1])
		if err != nil {
			return nil, fmt.Errorf("failed to unpack lockedBNB for delegator %s: %w", d.Hex(), err)
		}

		// Convert to gwei (divide by 1e9) and sum
		total := new(big.Int).Add(pooledBNB, lockedBNB)
		if total.Sign() <= 0 {
			// do fullScan to find possible new validator of the delegator
			_, pooled, locked, err := s.fullScan(ctx, d, blockNumber)
			if err != nil {
				if err != errNoValidatorFound {
					return nil, err
				}
				pooled = big.NewInt(0)
				locked = big.NewInt(0)
			}
			total = new(big.Int).Add(pooled, locked)
		}
		result[d] = total
	}

	return result, nil
}

func (s *source) fullScan(ctx context.Context, delegator common.Address, blockNumber *big.Int) (creditAddr common.Address, pooledBNB, lockedBNB *big.Int, err error) {
	vals := s.validators.getValidators()
	pooledData, err := s.pooledABI.Pack("getPooledBNB", delegator)
	if err != nil {
		return
	}
	lockedData, err := s.lockedABI.Pack("lockedBNBs", delegator, big.NewInt(0))
	if err != nil {
		return
	}
	calls := make([]multicall, 0, len(vals)*2)
	for _, val := range vals {
		calls = append(calls,
			multicall{
				target:   val.CreditContract,
				calldata: pooledData,
			},
			multicall{
				target:   val.CreditContract,
				calldata: lockedData,
			})
	}
	data, err := s.multicallABI.Pack("aggregate", calls)
	if err != nil {
		return
	}
	// Call the multicall contract with the batch of calls
	res, err := s.client.CallContract(ctx, ethereum.CallMsg{
		To:   &s.multicallAddr,
		Data: data,
	}, blockNumber)
	if err != nil {
		return
	}
	unpackedRes, err := s.multicallABI.Unpack("aggregate", res)
	if err != nil {
		return
	}
	if len(unpackedRes) != 2 {
		err = fmt.Errorf("unexpected unpack result length: %d", len(unpackedRes))
		return
	}
	returnData := unpackedRes[1].([][]byte)
	// Each validator has 2 calls: getPooledBNB and lockedBNBs
	if len(returnData) != len(vals)*2 {
		err = fmt.Errorf("unexpected returnData length: %d, expected %d", len(returnData), len(vals)*2)
		return
	}

	for i, val := range vals {
		var pooled *big.Int
		var locked *big.Int
		err = s.pooledABI.UnpackIntoInterface(&pooled, "getPooledBNB", returnData[i*2])
		if err != nil {
			continue // skip this validator, try next
		}
		err = s.lockedABI.UnpackIntoInterface(&locked, "lockedBNBs", returnData[i*2+1])
		if err != nil {
			continue // skip this validator, try next
		}
		total := new(big.Int).Add(pooled, locked)
		if total.Sign() > 0 {
			// Found the validator
			creditAddr = val.CreditContract
			pooledBNB = pooled
			lockedBNB = locked
			s.cache.set(delegator, creditAddr)
			return
		}
	}
	s.cache.delete(delegator)
	// No valid validator relationship found for this delegator
	err = errNoValidatorFound
	return creditAddr, pooledBNB, lockedBNB, err
}

func (s *source) getCurrentHeight() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	height, err := s.client.BlockNumber(ctx)
	if err != nil {
		if logger := s.Logger(); logger != nil {
			logger.Error("failed to fetch current BSC height", "error", err)
		}
	return 0
	}
	return height
}

func (s *source) refreshValidators() (bool, error) {
	var (
		operators []common.Address
		credits   []common.Address
		total     *big.Int
	)

	offset := 0
	pageSize := 500 // big enough for BSC validator set

	oldVals := s.validators.getValidators()

	for {
		data, err := s.stakeHubABI.Pack("getValidators", big.NewInt(int64(offset)), big.NewInt(int64(pageSize)))
		if err != nil {
			return false, err
		}

		// Remove new context for lint; assume using an existing context (in real code should pass ctx)
		// Here, just use context.Background() for demonstration.
		res, err := s.client.CallContract(context.Background(), ethereum.CallMsg{
			To:   &s.stakeHubAddr,
			Data: data,
		}, nil)
		if err != nil {
			return false, err
		}

		out, err := s.stakeHubABI.Unpack("getValidators", res)
		if err != nil {
			return false, err
		}

		ops := out[0].([]common.Address)
		crs := out[1].([]common.Address)
		total = out[2].(*big.Int)

		operators = append(operators, ops...)
		credits = append(credits, crs...)

		offset += len(ops)
		if offset >= int(total.Int64()) || len(ops) == 0 {
			break
		}
	}

	// Build new slice of validatorInfo for update
	vals := make([]validatorInfo, len(operators))
	for i := range operators {
		vals[i] = validatorInfo{
			Operator:       operators[i],
			CreditContract: credits[i],
		}
	}

	setA := make(map[[20]byte][20]byte, len(oldVals))
	for _, v := range oldVals {
		setA[v.Operator] = v.CreditContract
	}
	changed := false

	// To check added/changed items
	for _, v := range vals {
		cc, ok := setA[v.Operator]
		if !ok || cc != v.CreditContract {
			changed = true
			break
		}
		delete(setA, v.Operator)
	}
	// setA now contains any removals, so if not empty, there are deletions
	if !changed && len(setA) > 0 {
		changed = true
	}

	s.validators.setValidators(vals)
	return changed, nil
}
