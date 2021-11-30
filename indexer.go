package main

import (
	"log"
	"time"

	"github.com/awesomefly/simplefts/index"
	"github.com/awesomefly/simplefts/store"
)

func Index(dumpPath, indexPath string) {
	log.Println("Starting index...")

	start := time.Now()
	docs, err := store.LoadDocuments(dumpPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

	start = time.Now()
	idx := index.NewBTreeIndex(indexPath)
	idx.Add(docs)
	log.Printf("Indexed %d documents in %v", len(docs), time.Since(start))

	idx.Close()
}
