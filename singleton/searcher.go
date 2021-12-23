package singleton

import (
	"errors"
	"log"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	btree "github.com/awesomefly/gobtree"

	"github.com/awesomefly/easysearch/index"
	"github.com/awesomefly/easysearch/paraphrase/serving"
	"github.com/awesomefly/easysearch/util"
)

const (
	SmallSegment  = 1
	MiddleSegment = 2
	BigSegment    = 3
)

type RealtimeIndex struct {
	Index          index.HashMapIndex
	IncrementQueue chan index.Document
}

type DoubleBuffer struct {
	WriteIdx uint32
	errChan  chan error

	Indices  []*index.HashMapIndex
	DocQueue []chan index.Document
}

func NewDoubleBuffer() *DoubleBuffer {
	buf := DoubleBuffer{}
	atomic.StoreUint32(&buf.WriteIdx, 0)

	for i := 0; i < 2; i++ {
		idx := make(index.HashMapIndex)
		buf.Indices = append(buf.Indices, &idx)
		buf.DocQueue = append(buf.DocQueue, make(chan index.Document, 100))
	}

	buf.errChan = buf.Start()
	return &buf
}

func (b *DoubleBuffer) Start() chan error {
	errChan := make(chan error, 1)
	go func() {
		for {
			select {
			case err := <-errChan:
				log.Fatal(err)
			default:
				b.DoAdd()
			}
		}
	}()
	return errChan
}

func (b *DoubleBuffer) Stop() {
	b.errChan <- errors.New("clear")
}

func (b *DoubleBuffer) DoAdd() {
	writeIdx := atomic.LoadUint32(&b.WriteIdx)

	//单协程写，无需加锁
	idx := b.Indices[writeIdx]
	docs := make([]index.Document, 0)
	for {
		select {
		case doc := <-b.DocQueue[writeIdx]:
			docs = append(docs, doc)
			continue
		default:
			break
		}
		break
	}
	idx.Add(docs)

	if len(b.DocQueue[1-writeIdx]) > 100 {
		//fmt.Println("CurrentIdx Change.")
		b.Swap()
	}
}

func (b *DoubleBuffer) Add(doc index.Document) {
	writeIdx := atomic.LoadUint32(&b.WriteIdx)
	b.DocQueue[writeIdx] <- doc
	b.DocQueue[1-writeIdx] <- doc
}

func (b *DoubleBuffer) Swap() {
	writeIdx := atomic.LoadUint32(&b.WriteIdx)
	atomic.StoreUint32(&b.WriteIdx, 1-writeIdx)
}

func (b *DoubleBuffer) ReadIndex() *index.HashMapIndex {
	writeIdx := atomic.LoadUint32(&b.WriteIdx)
	return b.Indices[1-writeIdx]
}

func (b *DoubleBuffer) Flush() {
	writeIdx := atomic.LoadUint32(&b.WriteIdx)
	for {
		if len(b.DocQueue[writeIdx]) > 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}

	if len(b.DocQueue[1-writeIdx]) > 0 {
		b.Swap()
		writeIdx = atomic.LoadUint32(&b.WriteIdx)
		for {
			if len(b.DocQueue[writeIdx]) > 0 {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
	}
}

type Searcher struct {
	//磁盘持久化索引，全量索引; 考虑重建成本，可以把全量索引拆成多个小索引
	BigSegment    *index.BTreeIndex //大索引，多个中型索引合并成大索引 eg.月/年/全量索引
	MiddleSegment *index.BTreeIndex //中型锁，多个小索引合并到一个中型索引  eg.周索引
	//SmallSegment  *index.BTreeIndex //小型索引分片，实时索引合并到这里 eg.天索引

	Segment *index.BTreeIndex // 考虑重建成本，可以把全量索引拆成天/周/月/年多个索引, 每天可只重建前一天的天级索引成本小
	RTSegment unsafe.Pointer // 实时更新索引

	DeleteList []index.Doc //delete docs list.  update doc = delete old doc and create new one

	Model *serving.Model //todo: 移到search server更合适
}

func NewSearcher(file string) *Searcher {
	doubleBuffer := make([]*RealtimeIndex, 0, 2)
	for i := 0; i < 2; i++ {
		idx := RealtimeIndex{
			Index:          make(index.HashMapIndex),
			IncrementQueue: make(chan index.Document, 1000),
		}
		doubleBuffer = append(doubleBuffer, &idx)
	}

	srh := &Searcher{
		Segment:    index.NewBTreeIndex(file),
		RTSegment:  unsafe.Pointer(NewDoubleBuffer()),
		DeleteList: make([]index.Doc, 0),
		Model:      nil,
	}
	return srh
}

func (srh *Searcher) InitParaphrase(file string) {
	srh.Model = serving.NewModel(file)
}

func (srh *Searcher) paraphrase(texts []string, n int) []string {
	if srh.Model == nil {
		return nil
	}
	var (
		positive = texts
		negative []string
	)
	l := len(texts)
	sim := srh.Model.GetSimilar(positive, negative, l+n)
	return sim[l:]
}

// Add doc to index double-buffer async
// write need lock but read do not
func (srh *Searcher) Add(doc index.Document) {
	seg := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
	seg.Add(doc)
}

// Del doc from index
func (srh *Searcher) Del(doc index.Document) {
	//todo: 加锁 or 也放到double buffer
	srh.DeleteList = append(srh.DeleteList, index.Doc{ID: int32(doc.ID)})
	sort.Sort((*index.PostingList)(&srh.DeleteList))
}

// Drain realtime index to disk
func (srh *Searcher) Drain() {
	old := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
	atomic.StorePointer(&srh.RTSegment, unsafe.Pointer(NewDoubleBuffer()))

	old.Flush()

	for k, v := range *old.ReadIndex() {
		//todo:合并到主索引需要加锁
		dst := srh.Segment.Lookup(k, true)
		dst = append(dst, v...)
		sort.Sort(dst)

		key := &btree.TestKey{K: k}
		srh.Segment.BT.Insert(key, &dst)
	}
	srh.Segment.BT.Drain()

	old.Stop()
}

// Rollover small segment to middle segment and middle segment to big segment
func (srh *Searcher) Rollover() {
	//todo: merge tow segment to a new file, then swap it with big segment
	ch := srh.Segment.BT.FullSet()
	for {
		k := <-ch
		d := <-ch
		v := <-ch
		if k == nil || d == nil || v == nil {
			break
		}

		var src index.PostingList
		src.FromBytes(v)

		dst := srh.MiddleSegment.Lookup(string(k), true)
		dst = append(dst, src...)
		sort.Sort(dst)

		id, err := strconv.ParseInt(string(d), 10, 64) //TestKey.Docid()对应
		if err != nil {
			panic(err)
		}

		key := &btree.TestKey{K: string(k), Id: id}
		srh.MiddleSegment.BT.Insert(key, &dst)
	}

	srh.MiddleSegment.DocNum += srh.Segment.DocNum
	srh.MiddleSegment.Len += srh.Segment.Len

	srh.MiddleSegment.BT.Drain()
	srh.Segment.Clear()
}

// Swap segment index, use for rebuild index
func (srh *Searcher) Swap(file string, flag int) {
	newIndex := index.NewBTreeIndex(file)

	//todo: 原子操作
	var old *index.BTreeIndex
	switch flag {
	case SmallSegment:
		old = srh.Segment
		srh.Segment = newIndex
	case MiddleSegment:
		old = srh.MiddleSegment
		srh.MiddleSegment = newIndex
	case BigSegment:
		old = srh.BigSegment
		srh.BigSegment = newIndex
	}
	old.Clear()
}

//SearchTips 搜索提示
//Trie 适合英文词典，如果系统中存在大量字符串且这些字符串基本没有公共前缀，则相应的trie树将非常消耗内存（数据结构之trie树）
//Double Array Trie 适合做中文词典，内存占用小
func (srh *Searcher) SearchTips() []string {
	//todo: 支持trie树 or FST
	return nil
}

// Search queries the index for the given text.
// todo: 检索召回（bm25） -> 粗排sort(CTR by LR) -> 精排sort(CVR by DNN) -> topN(堆排序)
func (srh *Searcher) Search(text string) []index.Doc {
	//todo: 支持范围查找&前缀查找
	//参考：Lucene builds an inverted index using Skip-Lists on disk,
	//and then loads a mapping for the indexed terms into memory using a Finite State Transducer (FST).

	// todo: 支持向量检索
	//1. Query Rewrite todo:支持查询纠错，意图识别
	must := util.Analyze(text)        //1.1 文本预处理：分词、去除停用词
	should := srh.paraphrase(must, 3) //1.2 语义扩展，即近义词/含义相同等
	r := srh.Segment.Retrieval(must, should, nil, 10, 1000)

	seg := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
	i := seg.ReadIndex().Retrieval(must, should, nil, 10, 1000)
	d := srh.DeleteList

	//merge
	(*index.PostingList)(&r).Union(i)

	//filter
	(*index.PostingList)(&r).Filter(d)
	return r
}
