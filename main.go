package main

import (
	"flag"
	"log"
	"time"

	"github.com/awesomefly/simplefts/index"

	"github.com/awesomefly/simplefts/store"
)

func main() {
	var dumpPath, query string
	flag.StringVar(&dumpPath, "p", "/Users/bytedance/Downloads/enwiki-latest-abstract1.xml.gz", "wiki abstract dump path")
	flag.StringVar(&query, "q", "Small cat", "search query")
	flag.Parse()

	log.Println("Starting simplefts")

	start := time.Now()
	docs, err := store.LoadDocuments(dumpPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

	start = time.Now()
	idx := make(index.HashIndex)
	idx.Add(docs)
	log.Printf("Indexed %d documents in %v", len(docs), time.Since(start))

	start = time.Now()
	searcher := NewSearcher()
	matchedIDs := searcher.Search(query)
	log.Printf("Search found %d documents in %v", len(matchedIDs), time.Since(start))

	for _, id := range matchedIDs {
		doc := docs[id]
		log.Printf("%d\t%s\n", id, doc.Text)
	}
}
