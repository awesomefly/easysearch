package search

import (
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/xtgo/set"

	"github.com/RoaringBitmap/roaring"

	"github.com/awesomefly/easysearch/index"
	"github.com/awesomefly/easysearch/paraphrase/serving"
	"github.com/awesomefly/easysearch/util"
)

type IndexType int

const (
	FullIndex IndexType = iota
	AuxIndex
)

type MsgType int

const (
	STOP MsgType = iota
	FLUSH
)

type Message struct {
	MsgType MsgType
	Msg     string
}

type DoubleBuffer struct {
	CurrentIdx uint32 //current write index
	msgChan    chan Message

	Indices []*index.HashMapIndex
	Queues  []chan index.Document
}

func NewDoubleBuffer() *DoubleBuffer {
	buf := DoubleBuffer{}
	atomic.StoreUint32(&buf.CurrentIdx, 0)

	for i := 0; i < 2; i++ {
		idx := index.NewHashMapIndex()
		buf.Indices = append(buf.Indices, idx)
		buf.Queues = append(buf.Queues, make(chan index.Document, 100))
	}

	buf.msgChan = buf.Start()
	return &buf
}

func (b *DoubleBuffer) WithDataRange(timestamp int64) *DoubleBuffer {
	t := time.Now()
	if timestamp != 0 {
		t = time.Unix(timestamp, 0)
	}
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()

	t = t.AddDate(0, 0, 1)
	end := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()

	for i := 0; i < len(b.Indices); i++ {
		b.Indices[i].Property().SetDataRange(index.DataRange{Start: int(start), End: int(end)})
	}
	return b
}

func (b *DoubleBuffer) Start() chan Message {
	msgChan := make(chan Message, 10)
	go func() {
		for {
			select {
			case msg := <-msgChan:
				switch msg.MsgType {
				case STOP:
					log.Printf("stop double buffer. msg:%s\n", msg.Msg)
					return
				case FLUSH:
					b.DoFlush()
				}
			default:
				b.DoAdd()
			}
		}
	}()
	return msgChan
}

func (b *DoubleBuffer) Stop() {
	b.msgChan <- Message{
		MsgType: STOP,
		Msg:     "stop",
	}
}

// DoFlush unsafe
func (b *DoubleBuffer) DoFlush() {
	for i := 0; i < len(b.Indices); i++ {
		idx := b.Indices[i]
		docs := make([]index.Document, 0)
		for {
			select {
			case doc := <-b.Queues[i]:
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
	}
}

func (b *DoubleBuffer) DoAdd() {
	writeIdx := atomic.LoadUint32(&b.CurrentIdx)

	//单协程写，无需加锁
	idx := b.Indices[writeIdx]
	docs := make([]index.Document, 0)
	for {
		timeout := time.NewTimer(1 * time.Millisecond)
		select {
		case doc := <-b.Queues[writeIdx]:
			docs = append(docs, doc)
			continue
		case <-timeout.C:
			break
		}
		break
	}

	if len(docs) > 0 {
		idx.Add(docs)
	}

	if len(b.Queues[1-writeIdx]) > 10 {
		atomic.CompareAndSwapUint32(&b.CurrentIdx, writeIdx, 1-writeIdx)
		//适当sleep，让历史读写操作执行完，避免读写并发
		time.Sleep(100 * time.Millisecond)
	}
}

func (b *DoubleBuffer) Add(doc index.Document) {
	for i := 0; i < len(b.Queues); i++ {
		b.Queues[i] <- doc
	}
}

func (b *DoubleBuffer) ReadIndex() *index.HashMapIndex {
	writeIdx := atomic.LoadUint32(&b.CurrentIdx)
	return b.Indices[1-writeIdx]
}

func (b *DoubleBuffer) Flush() {
	b.msgChan <- Message{
		MsgType: FLUSH,
		Msg:     "force flush",
	}
}

func (b *DoubleBuffer) Clear() {
}

type IndexArray struct {
	lock    sync.RWMutex
	indices []*index.BTreeIndex
}

func NewIndexArray() *IndexArray {
	return &IndexArray{
		indices: make([]*index.BTreeIndex, 0),
	}
}

func (b *IndexArray) WithFile(file string) *IndexArray {
	idx := index.NewBTreeIndex(file)

	t := time.Now()
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
	t = t.AddDate(0, 0, 1)
	end := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
	idx.Property().SetDataRange(index.DataRange{Start: int(start), End: int(end)})

	b.lock.Lock()
	defer b.lock.Unlock()

	b.indices = append(b.indices, idx)
	return b
}

func (b *IndexArray) Indices() []*index.BTreeIndex {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// 将数据复制到新的切片空间中
	copyData := make([]*index.BTreeIndex, len(b.indices))
	copy(copyData, b.indices)
	return copyData
}

func (b *IndexArray) Add(idx *index.BTreeIndex) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.indices = append(b.indices, idx)
}

// Hit 查找包含dr的index
func (b *IndexArray) Hit(dr index.DataRange) *index.BTreeIndex {
	b.lock.RLock()
	defer b.lock.RUnlock()

	for i := 0; i < len(b.indices); i++ {
		r := b.indices[i].Property().DataRange()
		if dr.Start >= r.Start && dr.End <= r.End { //在index的range范围内
			return b.indices[i]
		}
	}
	return nil
}

func (b *IndexArray) Swap(old *index.BTreeIndex, new *index.BTreeIndex) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	for i := 0; i < len(b.indices); i++ {
		if b.indices[i] == old { //在index的range范围内
			b.indices[i] = new
			return true
		}
	}
	return false
}

// Evict 淘汰dr范围内的index
func (b *IndexArray) Evict(dr index.DataRange) []*index.BTreeIndex {
	b.lock.Lock()
	defer b.lock.Unlock()

	var evicts []*index.BTreeIndex
	for i := 0; i < len(b.indices); {
		r := b.indices[i].Property().DataRange()
		if dr.Start <= r.Start && dr.End >= r.End { //在dr范围内的所以index
			evicts = append(evicts, b.indices[i])
			b.indices = append(b.indices[:i], b.indices[i+1:]...) //删除元素i
		} else {
			i++
		}
	}
	return evicts
}

type Searcher struct {
	//全量索引/主索引，历史全量数据静态构建成本高
	fullIndex unsafe.Pointer

	// 辅助索引（auxiliary index），全量索引较大重建不方便，可以近期新增数据构建成增量索引。
	// eg.每天只对1天前的数据重建索引，当天数据构建成增量索引
	auxIndex unsafe.Pointer //*IndexArray 带时间段的索引数组

	// 临时索引（incremental index）支持实时更新索引,利用双buff在内存中构建,支持无锁并发读写；
	// 内存不足时合并到辅助索引
	incrIndex unsafe.Pointer

	//deleteList []index.Doc //delete docs list.  update doc = delete old doc and create new one
	//BloomFilter  *bloom.Filter //也可使用布谷鸟过滤器效率更高
	roaringFilter *roaring.Bitmap //todo：如何删除过期数据

	model *serving.ParaphraseModel //todo: 移到search server更合适

	indexFile string
}

func NewSearcher(file string) *Searcher {
	srh := &Searcher{
		fullIndex: unsafe.Pointer(index.NewBTreeIndex(file)),
		auxIndex:  unsafe.Pointer(NewIndexArray().WithFile(file + ".aux." + strconv.Itoa(int(time.Now().Unix())))),
		incrIndex: unsafe.Pointer(NewDoubleBuffer().WithDataRange(0)),
		//deleteList: make([]index.Doc, 0),
		//BloomFilter: bloom.New(10000, 1000),
		roaringFilter: roaring.New(),
		model:         nil,
		indexFile:     file,
	}
	return srh
}

func (srh *Searcher) InitParaphrase(file string) {
	srh.model = serving.NewModel(file)
}

func (srh *Searcher) Paraphrase(texts []string, n int) []string {
	if srh.model == nil {
		return nil
	}
	var (
		positive = texts
		negative []string
	)
	l := len(texts)
	sim := srh.model.GetSimilar(positive, negative, l+n)
	return sim[l:]
}

// Add doc to index double-buffer async
// write need lock but read do not
func (srh *Searcher) Add(doc index.Document) {
	incr := (*DoubleBuffer)(atomic.LoadPointer(&srh.incrIndex))

	//跨天，新建个增量索引
	end := incr.ReadIndex().Property().DataRange().End
	if doc.Timestamp > end {
		srh.Drain(end)
	}

	//可能触发Drain需要重新Load
	(*DoubleBuffer)(atomic.LoadPointer(&srh.incrIndex)).Add(doc)
}

// Del doc from index
func (srh *Searcher) Del(doc index.Document) {
	//todo: 加锁
	srh.roaringFilter.Add(uint32(doc.ID))
}

func (srh *Searcher) Count() int {
	a := (*index.BTreeIndex)(atomic.LoadPointer(&srh.fullIndex)).Property().DocNum()
	copyData := (*IndexArray)(atomic.LoadPointer(&srh.auxIndex)).Indices()
	for i := 0; i < len(copyData); i++ {
		a += copyData[i].Property().DocNum()
	}
	a += (*DoubleBuffer)(atomic.LoadPointer(&srh.incrIndex)).ReadIndex().Property().DocNum()
	return a
}

func (srh *Searcher) Clear() {
	(*index.BTreeIndex)(atomic.LoadPointer(&srh.fullIndex)).Clear()
	copyData := (*IndexArray)(atomic.LoadPointer(&srh.auxIndex)).Indices()
	for i := 0; i < len(copyData); i++ {
		copyData[i].Clear()
	}
}

// Drain incremental index to disk
// 实际的原地更新策略，需要PostingList末尾预留足够空间，否则大量PostingList需要移动效率更低
// 磁盘空间足够时使用再合并策略，实现简单且不影响并发，但需要足够的内存
func (srh *Searcher) Drain(timestamp int) {
	oldIncr := (*DoubleBuffer)(atomic.SwapPointer(&srh.incrIndex, unsafe.Pointer(NewDoubleBuffer().WithDataRange(int64(timestamp)))))
	go func() {
		//flush after sleep any second
		time.Sleep(100 * time.Millisecond)
		oldIncr.Flush()
		oldIncr.Stop()

		oldIncrDR := oldIncr.ReadIndex().Property().DataRange()
		auxIdxArray := (*IndexArray)(atomic.LoadPointer(&srh.auxIndex))
		oldAux := auxIdxArray.Hit(oldIncrDR)
		if oldAux != nil {
			//合并keys
			keys := make(sort.StringSlice, len(oldIncr.ReadIndex().Map())+int(oldAux.BT.Count()))
			for k := range oldIncr.ReadIndex().Map() {
				keys = append(keys, k)
			}
			ch := oldAux.BT.KeySet()
			for {
				key := <-ch
				if key == nil {
					break
				}
				keys = append(keys, string(key))
			}
			sort.Strings(keys)
			set.Uniq(keys)

			//合并到新索引
			newAux := index.NewBTreeIndex(srh.indexFile + ".aux." + strconv.Itoa(int(time.Now().Unix())))
			for i := 0; i < keys.Len(); i++ {
				key := keys[i]
				pl := oldAux.Lookup(key, false)
				if pl2 := oldIncr.ReadIndex().Get(key); pl2 != nil {
					pl = append(pl, pl2...)
				}
				if len(pl) > 0 {
					newAux.Insert(key, pl)
				}
			}
			newAux.SetProperty(*oldAux.Property())
			newAux.Property().SetDocNum(oldIncr.ReadIndex().Property().DocNum() + oldAux.Property().DocNum())
			newAux.Property().SetTokenCount(oldIncr.ReadIndex().Property().TokenCount() + oldAux.Property().TokenCount())
			newAux.BT.Drain()

			//oldAux = (*index.BTreeIndex)(atomic.SwapPointer(&srh.auxIndex, unsafe.Pointer(newAux)))
			if auxIdxArray.Swap(oldAux, newAux) {
				oldAux.Clear()
				oldIncr.Clear()
			}
		} else {
			idx := index.NewBTreeIndex(srh.indexFile + ".aux." + strconv.Itoa(oldIncrDR.Start))
			idx.Property().SetDataRange(oldIncrDR)
			auxIdxArray.Add(idx)
		}
	}()
}

// Load index, use for rebuild index
func (srh *Searcher) Load(file string, flag IndexType) {
	newIndex := index.NewBTreeIndex(file)
	auxIdxArray := (*IndexArray)(atomic.LoadPointer(&srh.auxIndex))

	evicts := auxIdxArray.Evict(newIndex.Property().DataRange())
	switch flag {
	case FullIndex:
		old := (*index.BTreeIndex)(atomic.SwapPointer(&srh.fullIndex, unsafe.Pointer(newIndex)))
		evicts = append(evicts, old)
	case AuxIndex:
		auxIdxArray.Add(newIndex) //如果先添加后淘汰，需要避免自身也被淘汰
		//old = (*index.BTreeIndex)(atomic.SwapPointer(&srh.auxIndex, unsafe.Pointer(newIndex)))
	}

	for i := 0; i < len(evicts); i++ {
		evicts[i].Clear()
	}
}

//SearchTips todo: 支持搜索提示
//Trie 适合英文词典，如果系统中存在大量字符串且这些字符串基本没有公共前缀，则相应的trie树将非常消耗内存（数据结构之trie树）
//Double Array Trie 适合做中文词典，内存占用小
func (srh *Searcher) SearchTips() []string {
	//支持trie树 or FST
	return nil
}

func (srh *Searcher) Retrieval(terms []string, ext []string, model index.SearchModel) []index.Doc {
	var result []index.Doc

	fullIdx := (*index.BTreeIndex)(atomic.LoadPointer(&srh.fullIndex))
	auxIdxArray := (*IndexArray)(atomic.LoadPointer(&srh.auxIndex))
	incrIdx := (*DoubleBuffer)(atomic.LoadPointer(&srh.incrIndex)).ReadIndex()

	result = fullIdx.Retrieval(terms, ext, nil, 10, 1000, model)

	copyData := auxIdxArray.Indices()
	for i := 0; i < len(copyData); i++ {
		y := copyData[i].Retrieval(terms, ext, nil, 10, 1000, model)
		(*index.PostingList)(&result).Union(y)
	}

	z := incrIdx.Retrieval(terms, ext, nil, 10, 1000, model)
	(*index.PostingList)(&result).Union(z)
	return result
}

//Filter deleted docs
func (srh *Searcher) Filter(docs []index.Doc) []index.Doc {
	var result []index.Doc
	for _, doc := range docs {
		hit := srh.roaringFilter.Contains(uint32(doc.ID))
		if !hit {
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
	ext := srh.Paraphrase(terms, 3)

	//2. todo:多路召回（传统检索+向量检索）
	r := srh.Retrieval(terms, ext, index.BM25)

	//3. 过滤已删除文档filter
	r = srh.Filter(r)
	return r
}
