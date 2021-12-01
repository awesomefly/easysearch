package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/awesomefly/simplefts/singleton"

	"github.com/awesomefly/simplefts/index"
)

func main() {
	f, _ := os.OpenFile("cpu.pprof", os.O_CREATE|os.O_RDWR, 0666)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	var dumpPath, indexPath, query, module string
	flag.StringVar(&module, "m", "searcher", "[indexer|searcher|merger]")

	//index
	flag.StringVar(&dumpPath, "p", "/Users/bytedance/Downloads/enwiki-latest-abstract18.xml.gz", "wiki abstract dump path")
	flag.StringVar(&indexPath, "i", "/Users/bytedance/go/src/github.com/simplefts/data/wiki_index", "index path")

	//search
	flag.StringVar(&query, "q", "Album Jordan", "search query")

	//merge
	var srcPath, dstPath string
	flag.StringVar(&srcPath, "s", "/Users/bytedance/go/src/github.com/simplefts/data/src_index", "merge from")
	flag.StringVar(&dstPath, "d", "/Users/bytedance/go/src/github.com/simplefts/data/dst_index", "merge to")

	flag.Parse()

	log.Println("Starting simple fts")

	if module == "indexer" {
		os.Remove(indexPath + ".idx")
		os.Remove(indexPath + ".kv")
		os.Remove(indexPath + ".sum")
		singleton.Index(dumpPath, indexPath) //todo: 构建索引耗时过长，性能分析下具体耗时原因
	} else if module == "searcher" {
		start := time.Now()
		docs, err := index.LoadDocuments(dumpPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

		start = time.Now()
		searcher := singleton.NewSearcher(indexPath)
		log.Printf("Source index loaded %d keys in %v", searcher.Segment.BT.Count(), time.Since(start))

		matched := searcher.Search(query)
		log.Printf("Search found %d documents in %v", len(matched), time.Since(start))

		for _, d := range matched {
			doc := docs[d.ID]
			log.Printf("%d\t%s\n", d.ID, doc.Text)
		}
	} else if module == "merger" {
		singleton.Merge(srcPath, dstPath)
	}
}
