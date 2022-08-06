package blockchain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/graviton"
)

func (s *orphanStorage) InitializeOrphanDB() (err error) {
	path := filepath.Join(globals.GetDataDirectory(), "orphans")
	s.Store, err = graviton.NewDiskStore(path)
	if err != nil {
		return
	}

	s.FirstHeight = LoadOrphanFirstHeight()

	return
}

func LoadOrphanFirstHeight() int64 {
	filepath := filepath.Join(globals.GetDataDirectory(), "orphan_first_height.csv")
	data, err := os.ReadFile(filepath)
	if err != nil {
		return 0
	}

	height, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0
	}
	return height
}

func (s *orphanStorage) writeFirstHeight(height int64) (err error) {
	filepath := filepath.Join(globals.GetDataDirectory(), "orphan_first_height.csv")

	heightString := strconv.FormatInt(height, 10)

	return os.WriteFile(filepath, []byte(heightString), 0644)
}

func (chain *Blockchain) storeOrphan(key []byte, value []byte) (err error) {
	ss, err := chain.OrphanDB.Store.LoadSnapshot(0)
	if err != nil {
		return
	}

	tree, err := ss.GetTree("orphans")
	if err != nil {
		return
	}

	err = tree.Put(key, value)
	if err != nil {
		return
	}

	_, err = graviton.Commit(tree)
	if err != nil {
		return
	}

	return
}

func (s *orphanStorage) getValueFromOrphanDB(key []byte) (value []byte, err error) {
	ss, err := s.Store.LoadSnapshot(0)
	if err != nil {
		return
	}

	tree, err := ss.GetTree("orphans")
	if err != nil {
		return
	}

	value, err = tree.Get(key)
	if err != nil {
		return
	}

	return
}

func (s *orphanStorage) GetOrphan(height int64) [][33]byte {
	v, err := s.getValueFromOrphanDB(serializeHeight(height))
	if err != nil {
		return nil
	}

	orphans, err := deserializeCompressedKeys(v)
	if err != nil {
		return nil
	}

	return orphans
}

func GetOrphan(tree *graviton.Tree, height int64) [][33]byte {
	value, err := tree.Get(serializeHeight(height))
	if err != nil {
		return nil
	}

	orphans, err := deserializeCompressedKeys(value)
	if err != nil {
		return nil
	}

	return orphans
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

func serializeHeight(height int64) []byte {
	var b bytes.Buffer
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(height))
	b.Write(buf[:n])

	return b.Bytes()
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

func deserializeHeight(buf []byte) (int64, error) {
	var err error
	height, done := binary.Uvarint(buf)
	if done <= 0 {
		err = fmt.Errorf("invalid height")
	}
	return int64(height), err
}

func deserializeCompressedKeys(buf []byte) (keys [][33]byte, err error) {
	done := 0
	count, done := binary.Uvarint(buf)
	if done <= 0 || done > 1 {
		err = fmt.Errorf("invalid count")
		return
	}
	buf = buf[done:]

	for i := uint64(0); i < count; i++ {
		var key [33]byte
		copy(key[:], buf[:33])
		keys = append(keys, key)

		buf = buf[33:]
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

func (s *orphanStorage) GetOrphanRateLastN(n int64, stableHeight int64) (rate float64) {
	if n < 1 || stableHeight < 1 {
		return
	}

	ss, err := s.Store.LoadSnapshot(0)
	if err != nil {
		return
	}

	tree, err := ss.GetTree("orphans")
	if err != nil {
		return
	}

	var sum, count uint64
	for i := stableHeight - 1; i >= (stableHeight-n) && i > 0; i-- {
		count++

		orphans := GetOrphan(tree, i)

		sum += uint64(len(orphans))
	}

	rate = float64(sum) / float64(count*9+sum) * 100.0

	return
}

// print orphan miniblocks info of upto 30 days (144000 blocks)
func (s *orphanStorage) OrphanInfo_Print(stableHeight int64) {
	logger.Info("Orphan Miniblock Info")

	if s.FirstHeight == 0 || s.FirstHeight == stableHeight {
		fmt.Println("\nNo data is available: 00%")
		return
	}

	ss, err := s.Store.LoadSnapshot(0)
	if err != nil {
		return
	}

	tree, err := ss.GetTree("orphans")
	if err != nil {
		return
	}

	blockCount := stableHeight - s.FirstHeight
	if blockCount > 144000 {
		blockCount = 144000
	}

	startHeight := stableHeight - blockCount
	endHeight := stableHeight - 1

	var totalCount uint64
	var hourlyCounts [24]uint64
	var dailyCounts [7]uint64
	var weeklyCounts [4]uint64

	for height := endHeight; height >= startHeight; height-- {
		orphans := GetOrphan(tree, height)

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
