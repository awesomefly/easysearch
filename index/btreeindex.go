//
// btree inverted index's data structure
//
// |<-------------btree------------>| <--posting list-->|
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
// |<--in memory--> | <-----------on disk-------------->|
//
//

package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/awesomefly/easysearch/util"
	btree "github.com/awesomefly/gobtree"
)

var DefaultConfig = btree.Config{
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      4,
	RebalanceThrs: 30,
	AppendRatio:   0.7,
	DrainRate:     100,
	MaxLeafCache:  0, // intermediate node in memory and leaf node in disk
	Sync:          false,
	Nocache:       false,
}

type BTreeIndex struct {
	//skip-list vs btree:
	//https://stackoverflow.com/questions/256511/skip-list-vs-binary-search-tree/28270537#28270537
	BT *btree.BTree

	// DocNum is the count of documents
	DocNum int

	// Len is the total length of docs
	Len int

	IndexFile string
}

func NewBTreeIndex(file string) *BTreeIndex {
	conf := DefaultConfig
	conf.Idxfile, conf.Kvfile = file+".idx", file+".kv"
	bt := BTreeIndex{
		IndexFile: file,
		BT:        btree.NewBTree(btree.NewStore(conf)), // todo: 索引文件太大，索引压缩、posting list压缩
	}

	bt.Load()
	return &bt
}

func (bt *BTreeIndex) Save() {
	file := bt.IndexFile + ".sum"
	os.Create(file)

	// Index store
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	buffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.DocNum)); err != nil {
		panic(err)
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.Len)); err != nil {
		panic(err)
	}

	if _, err := fd.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
	fd.Close()
}

func (bt *BTreeIndex) Load() {
	// Index store
	file := bt.IndexFile + ".sum"
	fd, err := os.OpenFile(file, os.O_RDONLY|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	data := make([]byte, unsafe.Sizeof(bt.DocNum)+unsafe.Sizeof(bt.Len))
	if n, err := fd.Read(data); err != nil {
		if n == 0 && err == io.EOF {
			return
		}
		panic(err.Error())
	}

	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.DocNum))); err != nil {
		panic(err.Error())
	}

	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.Len))); err != nil {
		panic(err.Error())
	}

	fd.Close()
}

func (bt *BTreeIndex) Close() {
	bt.BT.Drain()
	bt.BT.Close()
	bt.Save()
}

func (bt *BTreeIndex) Clear() {
	//todo: delete deprecated index
}

func (bt *BTreeIndex) Lookup(token string, dirty bool) PostingList {
	key := &btree.TestKey{K: token}

	var ch chan []byte
	if dirty {
		ch = bt.BT.LookupDirty(key)
	} else {
		ch = bt.BT.Lookup(key) //todo: btree内部节点key值直接查文件，用kdping缓存加速查询
	}
	values := make([][]byte, 0)
	for {
		x := <-ch
		if x == nil {
			break
		}
		values = append(values, x)
	}

	if len(values) == 0 {
		return nil
	}

	var p PostingList
	p.FromBytes(values[0])
	return p
}

func (bt *BTreeIndex) Add(docs []Document) {
	for _, doc := range docs {
		tokens := util.Analyze(doc.Text)
		for _, token := range tokens {
			//log.Printf("token:%s", token)

			key := &btree.TestKey{K: token}
			postingList := bt.Lookup(token, true)
			if postingList != nil {
				if last := &postingList[len(postingList)-1]; last.ID == int32(doc.ID) {
					// Don't add same ID twice. But should update frequency
					last.TF++
					last.Score = CalDocScore(last.TF, 0)
					bt.BT.Insert(key, postingList)
					continue
				}
			}
			item := Doc{
				ID:     int32(doc.ID),
				DocLen: int32(len(tokens)),
				TF:     1,
				Score:  CalDocScore(1, 0),
			}
			//add to posting list & sort by score
			//todo: 数组不适合频繁写，考虑其他数据结构优化，先找到
			postingList = append(postingList, item)
			sort.Slice(postingList, func(i, j int) bool {
				return postingList[i].Score > postingList[j].Score
			})
			bt.BT.Insert(key, postingList)
		}
		bt.DocNum++
		bt.Len += len(tokens) // todo: adding only unique words
	}
	bt.BT.Drain()
}

func (bt *BTreeIndex) Get(terms []string, r int) map[Term][]Doc {
	result := make(map[Term][]Doc, len(terms))
	for _, term := range terms {
		if postingList := bt.Lookup(term, false); postingList != nil {
			l := postingList[:IfElseInt(len(postingList) > r, r, len(postingList))] //胜者表按frequency排序,截断前r个,加速归并
			result[Term{K: term, DF: int32(len(postingList))}] = l
		} else {
			result[Term{K: term}] = nil
		}
	}
	return result
}

//VecSpaceRetrieval 向量空间检索
func (bt *BTreeIndex) VecSpaceRetrieval(terms []string, k int, r int) []Doc {
	tfidf := NewTFIDF()

	//query's term frequency
	queryDocId := int32(-1)
	tfidf.DOC[queryDocId] = make(TF, 0)

	var result PostingList
	for _, term := range terms {
		tfidf.DOC[queryDocId][term]++
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			//sort.Sort(plr)  //Union中会先sort在diff
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}

			tfidf.IDF[term] = CalIDF(bt.DocNum, len(pl))
			for _, doc := range plr {
				tf := tfidf.DOC[doc.ID]
				if tf == nil {
					tf = make(TF, 0)
				}
				tf[term] = doc.TF
				tfidf.DOC[doc.ID] = tf
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	result = CalCosine(result, tfidf)
	return result
}

//ProbRetrieval 概率检索模型，常用BM25
func (bt *BTreeIndex) ProbRetrieval(terms []string, k int, r int) []Doc {
	tfidf := NewTFIDF()

	var result PostingList
	for _, term := range terms {
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			// sort.Sort(plr)  Union中会先sort在diff
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}

			tfidf.IDF[term] = CalIDF(bt.DocNum, len(pl))
			for _, doc := range plr {
				tf := tfidf.DOC[doc.ID]
				if tf == nil {
					tf = make(TF, 0)
				}
				tf[term] = doc.TF
				tfidf.DOC[doc.ID] = tf
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	//log.Printf("result len:%d\n", len(result))
	result = CalBM25(result, tfidf, bt.Len, bt.DocNum)
	if len(result) > k {
		return result[:k]
	}
	return result
}

// BooleanRetrieval 布尔检索
func (bt *BTreeIndex) BooleanRetrieval(must []string, should []string, not []string, k int, r int) []Doc {
	var result PostingList
	for _, term := range must {
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按frequency排序,截断前r个,加速归并
			sort.Sort(plr)
			if result == nil {
				result = plr
			} else {
				result.Inter(plr)
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			sort.Sort(plr)
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl := bt.Lookup(term, false); pl != nil {
			sort.Sort(pl)
			result.Filter(pl)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	if len(result) > k {
		return result[:k]
	}
	return result
}
