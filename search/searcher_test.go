package search

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/awesomefly/easysearch/index"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func get(srh *Searcher, text string) []int {
	rt := (*DoubleBuffer)(atomic.LoadPointer(&srh.incrIndex))
	return (index.PostingList)(rt.ReadIndex().Get(text)).IDs()
}

var searcher = NewSearcher("../data/test_insread") //必须为全局变量
func BenchmarkDoubleBuffer(b *testing.B) {
	//b.N = 10000
	rand.Seed(time.Now().UnixNano())
	fmt.Println(runtime.GOMAXPROCS(0))

	fmt.Println("start")
	searcher.Add(index.Document{ID: 1, Text: "A donut on a glass plate. Only the donuts."})
	for i := 0; i < b.N; i++ {
		searcher.Add(index.Document{ID: 1, Text: randSeq(5)})
		get(searcher, "donut")
	}
	fmt.Println("done")
}

func BenchmarkDoubleBufferParallel(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	fmt.Println(runtime.GOMAXPROCS(0))
	searcher.Add(index.Document{ID: 1, Text: "A donut on a glass plate. Only the donuts."})

	// 测试一个对象或者函数在多线程的场景下面是否安全
	b.SetParallelism(10000) //协程总数：b.parallelism * runtime.GOMAXPROCS(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() { //每个协程运行b.N个case
			t := randSeq(5)
			searcher.Add(index.Document{ID: 1, Text: t})
			get(searcher, "donut")
			get(searcher, t)
		}
	})
}
func TestSearcherLoad(t *testing.T) {
	searcher.Add(index.Document{ID: 1, Text: "A donut on a glass plate.", Timestamp: int(time.Now().Unix())}) //当天的文档
	searcher.Add(index.Document{ID: 2, Text: "Only the donuts.", Timestamp: int(time.Now().AddDate(0,0,1).Unix())}) //第二天的文档
	time.Sleep(2 * time.Second)

	copyData :=(*IndexArray)(atomic.LoadPointer(&searcher.auxIndex)).Indices()
	fmt.Printf("1index len:%d\n", len(copyData))
	assert.Equal(t, 1, len(copyData)) //触发索引分裂，此时当天的索引文件已持久化


	searcher.Drain(0)
	time.Sleep(2 * time.Second)
	copyData =(*IndexArray)(atomic.LoadPointer(&searcher.auxIndex)).Indices()
	fmt.Printf("2index len:%d\n", len(copyData))
	assert.Equal(t, 2, len(copyData)) //手动持久化第二天的文档


	newIndex := index.NewBTreeIndex("../data/test_insread_xxx")

	ts := time.Now()
	start := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location()).Unix()
	ts = ts.AddDate(0, 0, 3)
	end := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location()).Unix()
	newIndex.Property().SetDataRange(index.DataRange{Start: int(start), End: int(end)})

	newIndex.Add([]index.Document{{ID: 3, Text: "god is girl."}})
	newIndex.Close()

	searcher.Load("../data/test_insread_xxx", AuxIndex)
	copyData =(*IndexArray)(atomic.LoadPointer(&searcher.auxIndex)).Indices()
	fmt.Printf("3index len:%d\n", len(copyData))
	assert.Equal(t, 1, len(copyData)) //手动持久化第二天的文档

	rst := searcher.Search("girl")
	fmt.Printf("%+v", rst)
	assert.Equal(t, 1, len(rst))

	searcher.Clear()
}

func TestSearcher(t *testing.T) {
	searcher.Add(index.Document{ID: 1, Text: "A donut on a glass plate. Only the donuts."})
	for i := 0; i < 12; i++ {
		searcher.Add(index.Document{ID: 10+i, Text: randSeq(5)})
	}
	time.Sleep(2 * time.Second)
	fmt.Printf("count:%d\n", searcher.Count())
	assert.Equal(t, 13, searcher.Count()) //默认10个doc会切换双buffer

	searcher.Drain(0)
	time.Sleep(2 * time.Second)

	var a int
	copyData :=(*IndexArray)(atomic.LoadPointer(&searcher.auxIndex)).Indices()
	for i := 0; i < len(copyData); i++ {
		a += copyData[i].Property().DocNum()
	}
	fmt.Printf("index len:%d\n", len(copyData))
	fmt.Printf("auxIndex count:%d\n", a)
	assert.Equal(t, 13, a)

	rst := searcher.Search("donut")
	fmt.Printf("%+v", rst)
	assert.Equal(t, 1, len(rst))

	//Del&Filter
	searcher.Del(index.Document{ID: 1})
	rst = searcher.Search("donut")
	assert.Equal(t, 0, len(rst))

	//Clear
	searcher.Clear()
}
