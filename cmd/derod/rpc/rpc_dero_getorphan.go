package rpc

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/rpc"
)

func GetOrphan(ctx context.Context, p rpc.GetOrphan_Params) (result rpc.GetOrphan_Result, err error) {
	defer func() { // safety so if anything wrong happens, we return error
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occured. stack trace %s", debug.Stack())
		}
	}()

	if p.Height < 0 {
		err = fmt.Errorf("height must be positive value")
		return
	}

	//if p.Height > chain.Get_Stable_Height() {
	if p.Height >= chain.Get_Stable_Height() { // have a margin because miniblocks might not be purged yet
		err = fmt.Errorf("user requested block at height more than (or equal to) chain stable height")
		return
	}

	orphans := chain.OrphanDB.GetOrphan(p.Height)

	if len(orphans) == 0 {
		result.Orphans = []string{}
		result.Status = "OK"
		return
	}

	for _, o := range orphans {
		if addr, err := rpc.NewAddressFromCompressedKeys(o[:]); err == nil {
			addr.Mainnet = globals.IsMainnet()
			result.Orphans = append(result.Orphans, addr.String())
		}

	}

	result.Status = "OK"

	return
}
