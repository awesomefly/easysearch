package main

import (
	"flag"
	"log"
	"time"

	"github.com/awesomefly/simplefts/store"
)

func main() {
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
		Index(dumpPath, indexPath)
	} else if module == "searcher" {
		start := time.Now()
		docs, err := store.LoadDocuments(dumpPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

		start = time.Now()
		searcher := NewSearcher(indexPath)
		log.Printf("Source index loaded %d keys in %v", searcher.PersistIndex.BT.Count(), time.Since(start))

		matchedIDs := searcher.Search(query)
		log.Printf("Search found %d documents in %v", len(matchedIDs), time.Since(start))

		for _, id := range matchedIDs {
			doc := docs[id]
			log.Printf("%d\t%s\n", id, doc.Text)
		}
	} else if module == "merger" {
		Merge(srcPath, dstPath)
	}
}
