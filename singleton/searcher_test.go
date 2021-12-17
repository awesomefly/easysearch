package singleton

import (
	"fmt"
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
	curIdx := atomic.LoadUint32(&srh.writeIdx)
	return srh.DoubleBuffer[1-curIdx].Index[text].IDs()
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
