package main

import (
	"runtime"
	"sort"
	"sync/atomic"

	btree "github.com/awesomefly/gobtree"

	"github.com/awesomefly/simplefts/common"
	"github.com/awesomefly/simplefts/index"
	"github.com/awesomefly/simplefts/store"
	"github.com/awesomefly/simplefts/word2vec"
)

type IncrementalIndex struct {
	Index          index.HashIndex
	IncrementQueue chan store.Document
}

type Searcher struct {
	//磁盘持久化索引，全量索引; 考虑重建成本，可以把全量索引拆成多个小索引
	//BigSegment    *index.BTreeIndex //大索引，多个中型索引合并成大索引 eg.月/索引
	//MiddleSegment *index.BTreeIndex //中型锁，多个小索引合并到一个中型索引  eg.周索引
	//SmallSegment  *index.BTreeIndex //小型索引分片，实时索引合并到这里 eg.天索引

	PersistIndex *index.BTreeIndex // todo: 考虑重建成本，可以把全量索引拆成天/周/月/年多个索引

	writeIdx           uint32
	writeIdxChangeLock chan bool
	DoubleBuffer       []*IncrementalIndex //DoubleBuffer索引，增量索引

	DeleteList []index.Doc //delete docs list.  update doc = delete old doc and create new one
}

func paraphrase(texts []string, n int) []string {
	path := "/Users/bytedance/go/src/github.com/simplefts/data/model.word2vec.format.bin"
	model := word2vec.Load(path)

	var (
		positive = texts
		negative []string
	)
	l := len(texts)
	sim := word2vec.GetSimilar(model, positive, negative, l+n)
	return sim[l:]
}

func NewSearcher(file string) *Searcher {
	doubleBuffer := make([]*IncrementalIndex, 0, 2)
	for i := 0; i < 2; i++ {
		idx := IncrementalIndex{
			Index:          make(index.HashIndex),
			IncrementQueue: make(chan store.Document, 1000),
		}
		doubleBuffer = append(doubleBuffer, &idx)
	}

	srh := &Searcher{
		PersistIndex:       index.NewBTreeIndex(file),
		DoubleBuffer:       doubleBuffer,
		DeleteList:         make([]index.Doc, 0),
		writeIdxChangeLock: make(chan bool, 1),
	}
	atomic.StoreUint32(&srh.writeIdx, 0)
	srh.createWriterWorker()
	return srh
}

func (srh *Searcher) createWriterWorker() {
	for p := 0; p < 2; p++ {
		go func() {
			for {
				curIdx := atomic.LoadUint32(&srh.writeIdx)
				if p != int(curIdx) {
					runtime.Gosched()
					continue
				}
				//单协程写，无需加锁
				buf := srh.DoubleBuffer[curIdx]
				doc := <-buf.IncrementQueue
				buf.Index.Add([]store.Document{doc})

				if len(srh.DoubleBuffer[1-curIdx].IncrementQueue) > 100 {
					//fmt.Println("CurrentIdx Change.")
					srh.writeIdxChangeLock <- true
					srh.changeCurrentIdx()
					<-srh.writeIdxChangeLock
				}
			}
		}()
	}
}

func (srh *Searcher) changeCurrentIdx() {
	curIdx := atomic.LoadUint32(&srh.writeIdx)
	atomic.StoreUint32(&srh.writeIdx, 1-curIdx)

}

// Add doc to index double-buffer async
// write need lock but read do not
func (srh *Searcher) Add(doc store.Document) {
	curIdx := atomic.LoadUint32(&srh.writeIdx)
	srh.DoubleBuffer[curIdx].IncrementQueue <- doc
	srh.DoubleBuffer[1-curIdx].IncrementQueue <- doc
}

// Del doc from index
func (srh *Searcher) Del(doc store.Document) {
	//todo: 加锁 or 也放到double buffer
	srh.DeleteList = append(srh.DeleteList, index.Doc{ID: int32(doc.ID)})
	sort.Sort((*index.PostingList)(&srh.DeleteList))
}

// Persist incremental index to disk
func (srh *Searcher) Persist() {
	srh.writeIdxChangeLock <- true //持久化过程中不允许double buffer切换
	writeIdx := atomic.LoadUint32(&srh.writeIdx)
	if writeIdx != 1 { //double-buffer[0]的数据比较新, 对double-buffer[0]持久化后可直接重置double-buffer
		srh.changeCurrentIdx()
	}

	for k, v := range srh.DoubleBuffer[1-writeIdx].Index {
		docs := make([]store.Document, 0)
		for _, id := range v.IDs() {
			docs = append(docs, store.Document{ID: id, Text: k})
		}

		//合并到主索引需要加锁
		dst := srh.PersistIndex.Lookup(k, true)
		dst = append(dst, v...)
		sort.Sort(dst)

		key := &btree.TestKey{K: k}
		srh.PersistIndex.BT.Insert(key, &dst)
	}
	srh.PersistIndex.BT.Drain()
	srh.DoubleBuffer[writeIdx].Index = make(index.HashIndex) //todo: 可升级为原子操作
	srh.DoubleBuffer[1-writeIdx].Index = make(index.HashIndex)
	<-srh.writeIdxChangeLock
}

// Search queries the index for the given text.
// todo: 检索召回（bm25） -> 粗排sort(CTR by LR) -> 精排sort(CVR by DNN) -> topN(堆排序)
func (srh *Searcher) Search(text string) []index.Doc {
	// todo: 支持向量检索
	must := common.Analyze(text)  // 分词
	should := paraphrase(must, 3) // 语义改写，即近义词扩展
	r := srh.PersistIndex.Retrieval(must, should, nil, 10, 1000)

	cur := atomic.LoadUint32(&srh.writeIdx)
	i := srh.DoubleBuffer[1-cur].Index.Retrieval(must, should, nil, 10, 1000)
	d := srh.DeleteList

	//merge
	(*index.PostingList)(&r).Union(i)

	//filter
	(*index.PostingList)(&r).Filter(d)
	return r
}
