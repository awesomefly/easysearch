package main

import (
	"sort"
	"sync"
	"sync/atomic"

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
	PersistIndex *index.BTreeIndex //磁盘持久化索引，全量索引 todo: 考虑重建成本，可以把全量索引拆成天/周/月/年多个索引

	WriteIdx     uint32
	DoubleBuffer []*IncrementalIndex //DoubleBuffer索引，增量索引

	DeleteList []int //delete docs list.  update doc = delete old doc and create new one
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

func NewSearcher() *Searcher {
	doubleBuffer := make([]*IncrementalIndex, 0, 2)
	for i := 0; i < 2; i++ {
		idx := IncrementalIndex{
			Index:          make(index.HashIndex),
			IncrementQueue: make(chan store.Document, 1000),
		}
		doubleBuffer = append(doubleBuffer, &idx)
	}

	srh := &Searcher{
		PersistIndex: index.NewBTreeIndex(),
		DoubleBuffer: doubleBuffer,
		DeleteList:   make([]int, 0),
	}
	atomic.StoreUint32(&srh.WriteIdx, 0)
	srh.createWriterWorker(5)
	return srh
}

func (srh *Searcher) createWriterWorker(numPros int) {
	var mutex sync.Mutex
	for p := 0; p < numPros; p++ {
		go func() {
			for {
				mutex.Lock() //todo: 优化为分段锁，否则相当于限制了写并发数为1
				curIdx := atomic.LoadUint32(&srh.WriteIdx)
				buf := srh.DoubleBuffer[curIdx]

				doc := <-buf.IncrementQueue
				buf.Index.Add([]store.Document{doc})
				if len(srh.DoubleBuffer[1-curIdx].IncrementQueue) > 100 {
					//fmt.Println("CurrentIdx Change.")
					srh.changeCurrentIdx()
				}
				mutex.Unlock()
			}
		}()
	}
}

func (srh *Searcher) changeCurrentIdx() {
	curIdx := atomic.LoadUint32(&srh.WriteIdx)
	atomic.StoreUint32(&srh.WriteIdx, 1-curIdx)
}

// Index create persistent index
// todo: 拆分 indexer 与 searcher
func (srh *Searcher) Index(docs []store.Document) {
	srh.PersistIndex.Add(docs)
}

// Add doc to index double-buffer async
// write need lock but read do not
func (srh *Searcher) Add(doc store.Document) {
	curIdx := atomic.LoadUint32(&srh.WriteIdx)
	srh.DoubleBuffer[curIdx].IncrementQueue <- doc
	srh.DoubleBuffer[1-curIdx].IncrementQueue <- doc
}

// Del doc from index
func (srh *Searcher) Del(doc store.Document) {
	//todo: 加锁
	srh.DeleteList = append(srh.DeleteList, doc.ID)
	sort.Sort(sort.IntSlice(srh.DeleteList))
}

func (srh *Searcher) get(text string) []int {
	curIdx := atomic.LoadUint32(&srh.WriteIdx)
	return srh.DoubleBuffer[1-curIdx].Index[text].IDs()
}

// persist incremental index to disk
func (srh *Searcher) persist() {
	curIdx := atomic.LoadUint32(&srh.WriteIdx)
	for k, v := range srh.DoubleBuffer[1-curIdx].Index {
		docs := make([]store.Document, 0)
		for _, id := range v.IDs() {
			docs = append(docs, store.Document{ID: id, Text: k})
		}
		srh.Index(docs) //todo: 合并到主索引需要加锁 or 保存成新的索引分片，查询时合并
	}
}

// Search queries the index for the given text.
// todo: multiply retrieval -> 粗排sort(CTR by LR) -> 精排sort(CVR by DNN) -> topN(堆排序)
func (srh *Searcher) Search(text string) []int {
	// todo: 向量检索
	must := common.Analyze(text)  // 分词
	should := paraphrase(must, 3) // 语义改写，即近义词扩展
	f := srh.PersistIndex.Retrieval(must, should, nil, 10, 1000)

	cur := atomic.LoadUint32(&srh.WriteIdx)
	i := srh.DoubleBuffer[1-cur].Index.Retrieval(must, should, nil, 10, 1000)
	d := srh.DeleteList

	//todo: 保留原序
	sort.Sort(sort.IntSlice(i))
	sort.Sort(sort.IntSlice(d))
	r := common.FilterInt(common.MergeInt(f, i), d)
	return r
}
