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

	var req_addr *rpc.Address
	if req_addr, err = rpc.NewAddress(strings.TrimSpace(p.Address)); err != nil {
		err = fmt.Errorf("Invalid Address")
		return
	}

	stringkeys := map[string]interface{}{}

	scid := crypto.HashHexToHash("0000000000000000000000000000000000000000000000000000000000000001")

	topoheight := chain.Load_TOPO_HEIGHT()

	if p.TopoHeight >= 1 {
		topoheight = p.TopoHeight
	}

	toporecord, err2 := chain.Store.Topo_store.Read(topoheight)
	// we must now fill in compressed ring members
	if err2 == nil {
		var ss *graviton.Snapshot
		ss, err2 = chain.Store.Balance_store.LoadSnapshot(toporecord.State_Version)
		if err2 == nil {
			var sc_data_tree *graviton.Tree
			sc_data_tree, err2 = ss.GetTree(string(scid[:]))
			if err2 == nil {
				// user requested all variables
				cursor := sc_data_tree.Cursor()
				var k, v []byte
				for k, v, err2 = cursor.First(); err2 == nil; k, v, err2 = cursor.Next() {
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
								stringkeys[vark.ValueString] = []byte(varv.ValueString)
							}

						case dvm.Uint64:

						default:
							err = fmt.Errorf("UNKNOWN Data type")
							return
						}
					}
				}
			}
		}
	}

	//fmt.Println(len(stringkeys))

	req_addr_raw := string(req_addr.Compressed())

	for k, v := range stringkeys {
		_, err3 := rpc.NewAddressFromCompressedKeys(v.([]byte))
		if err3 != nil {
			//fmt.Printf("%s, %x\n", k, v)
			continue
		}
		if req_addr_raw == string(v.([]byte)) {
			//fmt.Printf("%s, %x\n", k, v)
			result.Names = append(result.Names, k)
		}
	}

	if len(result.Names) != 0 {
		sort.Strings(result.Names)
		result.Address = strings.TrimSpace(p.Address)
		result.Status = "OK"
	} else {
		err = fmt.Errorf("Not Found")
	}

	return
}
