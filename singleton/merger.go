package singleton

import (
	"log"
	"sort"
	"strconv"
	"time"

	btree "github.com/awesomefly/gobtree"
	"github.com/awesomefly/simplefts/index"
)

func Merge(srcPath, dstPath string) {
	log.Println("Starting merge ...")

	start := time.Now()
	idx := index.NewBTreeIndex(srcPath)
	log.Printf("Source index loaded %d keys in %v", idx.BT.Count(), time.Since(start))

	start = time.Now()
	dstIdx := index.NewBTreeIndex(dstPath)
	log.Printf("Dst index loaded %d keys in %v", dstIdx.BT.Count(), time.Since(start))

	start = time.Now()
	ch := idx.BT.FullSet()
	for {
		k := <-ch
		d := <-ch
		v := <-ch
		if k == nil || d == nil || v == nil {
			break
		}

		var src index.PostingList
		src.FromBytes(v)

		dst := dstIdx.Lookup(string(k), true)
		dst = append(dst, src...)
		sort.Sort(dst)

		id, err := strconv.ParseInt(string(d), 10, 64) //TestKey.Docid()对应
		if err != nil {
			panic(err)
		}

		key := &btree.TestKey{K: string(k), Id: id}
		dstIdx.BT.Insert(key, &dst)
	}
	log.Printf("merge %s to %s in %v", srcPath, dstPath, time.Since(start))
	idx.Close()
	dstIdx.Close()
}
