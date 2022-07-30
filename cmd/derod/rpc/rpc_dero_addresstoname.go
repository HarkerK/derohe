package rpc

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/dvm"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/graviton"
)

func AddressToName(ctx context.Context, p rpc.AddressToName_Params) (result rpc.AddressToName_Result, err error) {

	defer func() { // safety so if anything wrong happens, we return error
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occured. stack trace %s", debug.Stack())
		}
	}()

	var addr *rpc.Address
	if addr, err = rpc.NewAddress(strings.TrimSpace(p.Address)); err != nil {
		err = fmt.Errorf("invalid address")
		return
	}
	addr_raw := string(addr.Compressed())

	scid := crypto.HashHexToHash("0000000000000000000000000000000000000000000000000000000000000001")

	topoheight := chain.Load_TOPO_HEIGHT()

	if p.TopoHeight >= 1 {
		topoheight = p.TopoHeight
	}

	toporecord, err := chain.Store.Topo_store.Read(topoheight)
	// we must now fill in compressed ring members
	if err == nil {
		var ss *graviton.Snapshot
		ss, err = chain.Store.Balance_store.LoadSnapshot(toporecord.State_Version)
		if err == nil {
			var sc_data_tree *graviton.Tree
			sc_data_tree, err = ss.GetTree(string(scid[:]))
			if err == nil {
				// user requested all variables
				cursor := sc_data_tree.Cursor()
				var k, v []byte
				for k, v, err = cursor.First(); err == nil; k, v, err = cursor.Next() {
					var vark, varv dvm.Variable

					_ = vark
					_ = varv
					_ = k
					_ = v

					//fmt.Printf("key '%x'  value '%x'\n", k, v)
					if k[len(k)-1] >= 0x3 && k[len(k)-1] < 0x80 && nil == vark.UnmarshalBinary(k) && nil == varv.UnmarshalBinary(v) {
						switch vark.Type {
						case dvm.String:
							if varv.Type == dvm.String {
								if varv.ValueString == addr_raw {
									result.Names = append(result.Names, vark.ValueString)
								}
							}

						case dvm.Uint64:

						default:
							err = fmt.Errorf("unknown data type")
							return
						}
					}
				}
			}
		}
	}

	if len(result.Names) != 0 {
		sort.Strings(result.Names)
		result.Address = addr.String()
		result.Status = "OK"
		err = nil
	} else {
		err = fmt.Errorf("not found")
	}

	return
}
