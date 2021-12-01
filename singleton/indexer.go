package singleton

import (
	"log"
	"time"

	"github.com/awesomefly/simplefts/index"
)

func Index(dumpPath, indexPath string) {
	log.Println("Starting index...")

	start := time.Now()
	docs, err := index.LoadDocuments(dumpPath)
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
