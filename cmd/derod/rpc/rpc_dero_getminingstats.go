package rpc

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/deroproject/derohe/blockchain"
	"github.com/deroproject/derohe/rpc"
)

func GetMiningStats(ctx context.Context, p rpc.GetMiningStats_Params) (result rpc.GetMiningStats_Result, err error) {
	defer func() { // safety so if anything wrong happens, we return error
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occured. stack trace %s", debug.Stack())
		}
	}()

	if p.StartHeight < 0 || p.StartHeight > p.EndHeight {
		err = fmt.Errorf("start height must be less than end height")
		return
	}

	StableHeight := chain.Get_Stable_Height()

	if p.EndHeight >= StableHeight {
		p.EndHeight = StableHeight - 1
	}

	// limit request
	if p.EndHeight-p.StartHeight >= 4800 {
		err = fmt.Errorf("request limit is 4800 blocks (1 day)")
		return
	}

	// if both Start Height and End Height are 0, give last 1 day (4800 blocks) data
	if p.StartHeight == 0 && p.EndHeight == 0 {
		p.StartHeight = StableHeight - 4800
		if p.StartHeight < 0 {
			p.StartHeight = 0
		}
		p.EndHeight = StableHeight - 1
	}

	var addr *rpc.Address
	if addr, err = rpc.NewAddress(strings.TrimSpace(p.Address)); err != nil {
		err = fmt.Errorf("invalid address")
		return
	}
	addr_raw := string(addr.Compressed())

	ss, err := chain.OrphanDB.Store.LoadSnapshot(0)
	if err != nil {
		return
	}

	tree, err := ss.GetTree("orphans")
	if err != nil {
		return
	}

	var i int64
	for i = p.StartHeight; i <= p.EndHeight; i++ {
		mkeys, err := chain.GetMinerKeys(i)
		if err != nil {
			continue
		}

		for i, mkey := range mkeys {
			if string(mkey[:]) == addr_raw {
				if i == len(mkeys)-1 {
					result.BlockCount++
				} else {
					result.MiniblockCount++
				}
			}
		}

		orphans := blockchain.GetOrphan(tree, i)

		for _, orphan := range orphans {
			if string(orphan[:]) == addr_raw {
				result.OrphanCount++
			}
		}
	}

	result.Address = addr.String()
	result.StartHeight = p.StartHeight
	result.EndHeight = p.EndHeight
	result.OrphanRate = float64(result.OrphanCount) / float64(result.MiniblockCount+result.OrphanCount) * 100
	result.Status = "OK"
	err = nil

	return
}
