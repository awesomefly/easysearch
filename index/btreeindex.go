//
// btree inverted index's data structure
//
// |<-------------btree------------>| <--posting list--> |
// |<-intermediate->|<--leaf node-->|
//				 -     --- ---          --- --- --- ---
//				| | - |   |   |    -   |   |   |   |   |
//               -     --- ---          --- --- --- ---
//			  /
//		   -     -     --- ---          --- --- --- ---
//        | | - | | - |   |   |    -   |   |   |   |   |
//		   -     -	   --- ---          --- --- --- ---
//		/
//   -     -     -     --- ---          --- --- --- ---
//	| | - | | - | | - |   |   |    -   |   |   |   |   |
//	 -	   -     -     --- ---          --- --- --- ---
//		\
//		   -     -     --- ---          --- --- --- ---
//        | | - | | - |   |   |    -   |   |   |   |   |
//		   -	 -     --- ---          --- --- --- ---
//| <--in memory--> | <-----------on disk--------------->|
//
//

package index

import (
	"bytes"
	"encoding/binary"
	"sort"

	btree "github.com/awesomefly/gobtree"
	"github.com/awesomefly/simplefts/common"
	"github.com/awesomefly/simplefts/score"
	"github.com/awesomefly/simplefts/store"
)

type BTreeIndex struct {
	BT *btree.BTree // todo: 目前key的值存储在文件中，用缓存加速查询
}

var conf = btree.Config{
	Idxfile: "/Users/bytedance/go/src/github.com/simplefts/data/test_insread_index.dat",
	Kvfile:  "/Users/bytedance/go/src/github.com/simplefts/data/test_insread_kv.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      4,
	RebalanceThrs: 30,
	AppendRatio:   0.7,
	DrainRate:     200,
	MaxLeafCache:  0, // intermediate node in memory and leaf node in disk
	Sync:          true,
	Nocache:       true,
}

type BTreeKey struct {
	K  string
	Id int64
}

func NewBTreeIndex() *BTreeIndex {
	bt := BTreeIndex{
		BT: btree.NewBTree(btree.NewStore(conf)),
	}
	return &bt
}
func (bt *BTreeIndex) Get(token string) PostingList {
	key := &btree.TestKey{K: token}
	buf := <-bt.BT.Lookup(key) //todo: btree内部节点key值直接查文件，这里可以加缓存
	if buf == nil {
		return nil
	}
	return FromBytes(buf)
}
func (bt *BTreeIndex) Add(docs []store.Document) {
	var tokenID int
	for _, doc := range docs {
		var ts []int
		for _, token := range common.Analyze(doc.Text) {
			//tfidf doc's token id list
			if _, ok := TokenCorpus[token]; !ok {
				TokenCorpus[token] = tokenID
				tokenID++
			}
			ts = append(ts, TokenCorpus[token])

			key := &btree.TestKey{K: token, Id: int64(TokenCorpus[token])}
			postingList := bt.Get(token)
			if postingList != nil {
				if last := &postingList[len(postingList)-1]; last.ID == doc.ID {
					// Don't add same ID twice. But should update frequency
					last.Frequency++
					last.Score = CalDocScore(last.Frequency, 0)
					continue
				}
			}
			item := PostingItem{
				ID:        doc.ID,
				Frequency: 1,
				Score:     CalDocScore(1, 0),
			}
			//add to posting list
			postingList = append(postingList, item)
			//sort.Sort(postingList) //sort by score
			bt.BT.Insert(key, &postingList)
			bt.BT.Drain() // todo: 优化下写文件效率, ps 每次insert都需要调用drain函数sync数据到磁盘
		}
		DocCorpus[doc.ID] = ts
	}

	//sort by score
	ch := bt.BT.FullSet()
	for {
		k := <-ch
		d := <-ch
		v := <-ch
		if k == nil || d == nil || v == nil {
			break
		}
		nv := FromBytes(v)
		sort.Sort(nv)
		key := &btree.TestKey{K: string(k), Id: BytesToInt64(d)}
		bt.BT.Insert(key, &nv)
	}
	bt.BT.Drain()
}

func (bt *BTreeIndex) Retrieval(must []string, should []string, not []string, k int, r int) []int {
	var result []int
	for _, term := range must {
		if pl := bt.Get(term); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按frequency排序,截断前r个,加速归并
			if result == nil {
				result = plr.IDs()
			} else {
				result = common.InterInt(result, plr.IDs())
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		if pl := bt.Get(term); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			if result == nil {
				result = plr.IDs() //胜者表，截断r
			} else {
				result = common.MergeInt(result, plr.IDs())
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl := bt.Get(term); pl != nil {
			result = common.FilterInt(result, pl.IDs())
		} else {
			// Token doesn't exist.
			continue
		}
	}

	if len(result) == 0 {
		return result
	}

	return score.MostSimilar(DocCorpus, TokenCorpus, must, result, k)
}

func BytesToInt64(bys []byte) int64 {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return data
}
