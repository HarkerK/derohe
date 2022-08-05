package blockchain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type orphanDB struct {
	DB *leveldb.DB
}

func (s *orphanDB) InitializeOrphanDB() (err error) {
	path := filepath.Join(globals.GetDataDirectory(), "orphans")
	s.DB, err = leveldb.OpenFile(path, nil)

	return
}

func (s *orphanDB) isEmpty() bool {
	isEmpty := false
	iter := s.DB.NewIterator(nil, nil)
	defer iter.Release()
	if !iter.Next() {
		isEmpty = true
	}

	return isEmpty
}

func (chain *Blockchain) isKeyExist(height int64) bool {
	isKeyExist := false
	_, err := chain.OrphanDB.DB.Get(serializeHeight(height), nil)
	if err == nil {
		isKeyExist = true
	}

	return isKeyExist
}

func (chain *Blockchain) storeOrphan(key []byte, value []byte) (err error) {
	chain.OrphanDB.DB.Put(key, value, nil)

	return
}

func (chain *Blockchain) GetOrphan(height int64) [][33]byte {
	v, err := chain.OrphanDB.DB.Get(serializeHeight(height), nil)
	if err != nil {
		return nil
	}

	orphans := deserializeCompressedKeys(v)

	return orphans
}

func (s *orphanDB) getFirstHeight() int64 {
	iter := s.DB.NewIterator(nil, nil)
	defer iter.Release()
	iter.Next()
	return deserializeHeight(iter.Key())
}

func serializeHeight(height int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(height))

	return b
}

func serializeCompressedKeys(keys [][33]byte) []byte {
	var b bytes.Buffer
	buf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutUvarint(buf, uint64(len(keys)))
	b.Write(buf[:n])

	for _, k := range keys {
		b.Write(k[:])
	}

	return b.Bytes()
}

func deserializeHeight(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func deserializeCompressedKeys(buf []byte) (keys [][33]byte) {
	done := 0
	count, done := binary.Uvarint(buf)
	/*
		if done <= 0 || done > 1 {
			err = fmt.Errorf("invalid count")
			return
		}
	*/
	buf = buf[done:]

	for i := uint64(0); i < count; i++ {
		var key [33]byte
		copy(key[:], buf[:33])
		keys = append(keys, key)

		buf = buf[33:]
	}

	return
}

func (chain *Blockchain) miniBlockMinerKeys(mbls []block.MiniBlock, chain_height int64) (keys [][33]byte) {
	if toporecord, err := chain.Store.Topo_store.Read(chain_height); err == nil {
		if ss, err := chain.Store.Balance_store.LoadSnapshot(toporecord.State_Version); err == nil {
			if balance_tree, err := ss.GetTree(config.BALANCE_TREE); err == nil {

				for _, mbl := range mbls {
					bits, key, _, err := balance_tree.GetKeyValueFromHash(mbl.KeyHash[0:16])
					if err != nil || bits >= 120 {
						continue
					}
					var k [33]byte
					copy(k[:], key)
					keys = append(keys, k)
				}
			}
		}
	}
	return
}

func (chain *Blockchain) GetMinerKeys(height int64) (keys [][33]byte, err error) {
	if height > chain.Load_TOPO_HEIGHT() {
		err = fmt.Errorf("user requested block at topoheight more than chain topoheight")
		return
	}

	hash, err := chain.Load_Block_Topological_order_at_index(height)
	if err != nil { // if err return err
		err = fmt.Errorf("User requested %d height block, chain height %d but err occured %s", height, chain.Get_Height(), err)
		return
	}

	bl, err := chain.Load_BL_FROM_ID(hash)
	if err != nil { // if err return err
		return
	}

	keys = chain.miniBlockMinerKeys(bl.MiniBlocks, height)
	keys = append(keys, bl.Miner_TX.MinerAddress)

	return
}

func (chain *Blockchain) GetOrphanCountRange(start, end int64) int64 {
	var count int64

	iter := chain.OrphanDB.DB.NewIterator(&util.Range{
		Start: serializeHeight(start),
		Limit: serializeHeight(end + 1),
	}, nil)

	for iter.Next() {
		orphans := deserializeCompressedKeys(iter.Value())
		count += int64(len(orphans))
	}
	iter.Release()

	return count
}

func (chain *Blockchain) GetOrphanCountRangeAddress(addr_raw string, start, end int64) int64 {
	var count int64

	iter := chain.OrphanDB.DB.NewIterator(&util.Range{
		Start: serializeHeight(start),
		Limit: serializeHeight(end + 1),
	}, nil)

	for iter.Next() {
		orphans := deserializeCompressedKeys(iter.Value())
		for _, o := range orphans {
			if string(o[:]) == addr_raw {
				count += int64(len(orphans))
			}
		}
	}
	iter.Release()

	return count
}

func (chain *Blockchain) GetOrphanRateLastN(n int64, stableHeight int64) (rate float64) {
	if n < 1 || stableHeight < 1 {
		return
	}

	if chain.OrphanDB.isEmpty() {
		return
	}

	start := int64(math.Max(float64(stableHeight-n), 0))
	end := stableHeight - 1
	blockCount := end - start + 1

	count := chain.GetOrphanCountRange(start, end)

	rate = float64(count) / float64(blockCount*9+count) * 100.0

	return
}

// print orphan miniblocks info of upto 30 days (144000 blocks)
func (chain *Blockchain) OrphanInfo_Print(stableHeight int64) {
	logger.Info("Orphan Miniblock Info")

	if chain.OrphanDB.isEmpty() {
		fmt.Println("\nNo data is available: 00%")
		return
	}

	blockCount := stableHeight - chain.OrphanDB.getFirstHeight()
	if blockCount > 144000 {
		blockCount = 144000
	}

	if blockCount == 0 {
		fmt.Println("\nNo data is available: 00%")
		return
	}

	startHeight := stableHeight - blockCount
	endHeight := stableHeight - 1

	var totalCount uint64
	var hourlyCounts [24]uint64
	var dailyCounts [7]uint64
	var weeklyCounts [4]uint64

	iter := chain.OrphanDB.DB.NewIterator(&util.Range{
		Start: serializeHeight(startHeight),
		Limit: serializeHeight(endHeight + 1),
	}, nil)

	for iter.Next() {
		height := deserializeHeight(iter.Key())
		orphans := deserializeCompressedKeys(iter.Value())

		if len(orphans) == 0 {
			continue
		}

		totalCount += uint64(len(orphans))

		for i := int64(0); i < 24; i++ {
			if stableHeight-200*(i+1) <= height && height < stableHeight-200*i {
				hourlyCounts[i] += uint64(len(orphans))
			}
		}

		for i := int64(0); i < 7; i++ {
			if stableHeight-4800*(i+1) <= height && height < stableHeight-4800*i {
				dailyCounts[i] += uint64(len(orphans))
			}
		}

		for i := int64(0); i < 4; i++ {
			if stableHeight-33600*(i+1) <= height && height < stableHeight-33600*i {
				weeklyCounts[i] += uint64(len(orphans))
			}
		}
	}
	iter.Release()

	totalMblsCount := uint64(blockCount*9) + totalCount
	totalRate := float64(totalCount) / float64(totalMblsCount) * 100.0

	fmt.Printf("\nTotal    : %6.2f%% [%d/%d] height %d to %d\n\n", totalRate, totalCount, totalMblsCount, startHeight, endHeight)

	for i := int64(0); i < 24; i++ {
		if blockCount >= 200*(i+1) {
			hourRate := float64(hourlyCounts[i]) / float64(1800+hourlyCounts[i]) * 100.0
			fmt.Printf("Hourly %2d: %6.2f%% [%d/%d] height %d to %d\n", i+1, hourRate, hourlyCounts[i], (1800 + hourlyCounts[i]), stableHeight-200*(i+1), stableHeight-200*i-1)
		} else {
			fmt.Printf("Hourly %2d: ------%% [--/--] height -- to --\n", i+1)
		}
	}

	fmt.Println()

	for i := int64(0); i < 7; i++ {
		if blockCount >= 4800*(i+1) {
			dayRate := float64(dailyCounts[i]) / float64(43200+dailyCounts[i]) * 100.0
			fmt.Printf("Daily  %2d: %6.2f%% [%d/%d] height %d to %d\n", i+1, dayRate, dailyCounts[i], (43200 + dailyCounts[i]), stableHeight-4800*(i+1), stableHeight-4800*i-1)
		} else {
			fmt.Printf("Daily  %2d: ------%% [--/--] height -- to --\n", i+1)
		}
	}

	fmt.Println()

	for i := int64(0); i < 4; i++ {
		if blockCount >= 33600*(i+1) {
			dayRate := float64(weeklyCounts[i]) / float64(302400+weeklyCounts[i]) * 100.0
			fmt.Printf("Weekly %2d: %6.2f%% [%d/%d] height %d to %d\n", i+1, dayRate, weeklyCounts[i], (302400 + weeklyCounts[i]), stableHeight-33600*(i+1), stableHeight-33600*i-1)
		} else {
			fmt.Printf("Weekly %2d: ------%% [--/--] height -- to --\n", i+1)
		}
	}
}

func Prune_Orphans(height int64) (err error) {
	path := filepath.Join(globals.GetDataDirectory(), "orphans")

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return
	}
	defer db.Close()

	iter := db.NewIterator(&util.Range{
		Start: nil,
		Limit: serializeHeight(height + 1),
	}, nil)

	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	return db.Write(batch, nil)

	//globals.Logger.Info("old orphan tree", "size", ByteCountIEC(DirSize(old_path)))
	//globals.Logger.Info("orphan tree after pruning", "size", ByteCountIEC(DirSize(new_path)))
	//os.RemoveAll(old_path)
	//return os.Rename(new_path, old_path)
}
