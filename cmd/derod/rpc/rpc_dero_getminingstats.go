package rpc

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

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

	if p.EndHeight > chain.Get_Stable_Height() {
		err = fmt.Errorf("user requested block at height more than chain stable height")
		return
	}

	var addr *rpc.Address
	if addr, err = rpc.NewAddress(strings.TrimSpace(p.Address)); err != nil {
		err = fmt.Errorf("invalid address")
		return
	}
	addr_raw := string(addr.Compressed())

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

		orphans := chain.GetOrphan(i)

		for _, orphan := range orphans {
			if string(orphan[:]) == addr_raw {
				result.OrphanCount++
			}
		}
	}

	result.Address = addr.String()
	result.OrphanRate = float64(result.OrphanCount) / float64(result.MiniblockCount+result.OrphanCount) * 100
	result.Status = "OK"
	err = nil

	return
}
