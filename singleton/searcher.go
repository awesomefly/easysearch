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
	"github.com/yourbasic/bloom"

	"github.com/awesomefly/easysearch/index"
	"github.com/awesomefly/easysearch/paraphrase/serving"
	"github.com/awesomefly/easysearch/util"
)

type SegmentType int
const (
	SmallSegment  SegmentType = iota
	MiddleSegment
	BigSegment
)

type SearchModel int
const (
	Boolean  SearchModel = iota
	VectorSpace
	BM25
)

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
		idx := index.NewHashMapIndex()
		buf.Indices = append(buf.Indices, idx)
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

	if len(docs) > 0 {
		idx.Add(docs)
	}

	if len(b.DocQueue[1-writeIdx]) > 10 {
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
	BloomFilter  *bloom.Filter

	Model *serving.Model //todo: 移到search server更合适
}

func NewSearcher(file string) *Searcher {
	srh := &Searcher{
		Segment:    index.NewBTreeIndex(file),
		RTSegment:  unsafe.Pointer(NewDoubleBuffer()),
		DeleteList: make([]index.Doc, 0),
		BloomFilter:bloom.New(10000, 1000),
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
	srh.BloomFilter.Add(strconv.Itoa(doc.ID))
}

// Drain realtime index to disk
func (srh *Searcher) Drain() {
	old := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
	atomic.StorePointer(&srh.RTSegment, unsafe.Pointer(NewDoubleBuffer()))

	old.Flush()

	for k, v := range old.ReadIndex().Map {
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
func (srh *Searcher) Swap(file string, flag SegmentType) {
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

//SearchTips todo: 支持搜索提示
//Trie 适合英文词典，如果系统中存在大量字符串且这些字符串基本没有公共前缀，则相应的trie树将非常消耗内存（数据结构之trie树）
//Double Array Trie 适合做中文词典，内存占用小
func (srh *Searcher) SearchTips() []string {
	//支持trie树 or FST
	return nil
}

func (srh *Searcher)Retrieval(terms []string, ext []string, model SearchModel) []index.Doc {
	var result []index.Doc
	switch model {
	case Boolean:
		result = srh.Segment.BooleanRetrieval(terms, ext, nil, 10, 1000)

		seg := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
		y := seg.ReadIndex().BooleanRetrieval(terms, ext, nil, 10, 1000)

		//merge
		result = append(result, y...)
	case VectorSpace:
		terms = append(terms, ext...)
		result = srh.Segment.VecSpaceRetrieval(terms, 10, 1000)

		seg := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
		y := seg.ReadIndex().VecSpaceRetrieval(terms,10, 1000)

		//merge
		result = append(result, y...)
		sort.Slice(result, func(i, j int) bool {
			return result[i].Cosine > result[j].Cosine //降序
		})
	case BM25:
		terms = append(terms, ext...)
		result = srh.Segment.ProbRetrieval(terms, 10, 1000)

		seg := (*DoubleBuffer)(atomic.LoadPointer(&srh.RTSegment))
		y := seg.ReadIndex().ProbRetrieval(terms,10, 1000)

		//merge
		result = append(result, y...)
		sort.Slice(result, func(i, j int) bool {
			return result[i].BM25 > result[j].BM25 //降序
		})
	}
	return result
}

//Filter deleted docs
func (srh *Searcher) Filter(docs []index.Doc) []index.Doc {
	var result []index.Doc
	for _, doc := range docs {
		if !srh.BloomFilter.Test(strconv.Itoa(int(doc.ID))) {
			result = append(result, doc)
		}
	}
	return result
}

// Search queries the index for the given text.
// todo: 检索召回（多路召回） -> 粗排sort(CTR by LR) -> 精排sort(CVR by DNN) -> topN(堆排序)
func (srh *Searcher) Search(query string) []index.Doc {
	//todo: 支持前缀查找
	//参考：Lucene builds an inverted index using Skip-Lists on disk,
	//and then loads a mapping for the indexed terms into memory using a Finite State Transducer (FST).

	//1. Query Rewrite todo:支持查询纠错，意图识别
	//1.1 文本预处理：分词、去除停用词、词干提取
	terms := util.Analyze(query)
	//1.2 语义扩展，即近义词/含义相同等
	ext := srh.paraphrase(terms, 3)

	//2. todo:多路召回（传统检索+向量检索）
	r := srh.Retrieval(terms, ext, BM25)

	//3. 过滤已删除文档filter
	r = srh.Filter(r)
	return r
}
