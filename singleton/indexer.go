package singleton

import (
	"log"
	"os"
	"time"

	"github.com/awesomefly/easysearch/config"

	"github.com/awesomefly/easysearch/index"
)

func Index(c config.Config) {
	log.Println("Starting index...")
	start := time.Now()
	docs, err := index.LoadDocuments(c.Store.DumpFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

	os.Remove(c.Store.IndexFile + ".idx")
	os.Remove(c.Store.IndexFile + ".kv")
	os.Remove(c.Store.IndexFile + ".sum")

	start = time.Now()
	idx := index.NewBTreeIndex(c.Store.IndexFile)
	idx.Add(docs)
	log.Printf("Indexed %d documents and %d keys in %v", len(docs), idx.BT.Count(), time.Since(start))

	idx.BT.Stats(true)
	idx.Close()
}
