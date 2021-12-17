package cluster

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/awesomefly/easysearch/config"

	"github.com/awesomefly/easysearch/index"
)

func Index(conf *config.Config) {
	log.Println("Starting index...")

	start := time.Now()
	docs, err := index.LoadDocuments(conf.Store.DumpFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d documents in %v", len(docs), time.Since(start))

	shards := conf.Cluster.ShardingNum
	idxes := make([]*index.BTreeIndex, 0, shards)
	for i := 0; i < shards; i++ {
		IndexFile := fmt.Sprintf("%s.%d", conf.Store.IndexFile, i)
		os.Remove(IndexFile + ".idx")
		os.Remove(IndexFile + ".kv")
		os.Remove(IndexFile + ".sum")

		idx := index.NewBTreeIndex(IndexFile)
		idxes = append(idxes, idx)
	}

	buf := make([][]index.Document, shards)
	for i := 0; i < len(buf); i++ {
		buf[i] = make([]index.Document, 0)
	}

	start = time.Now()
	for i := 0; i < len(docs); i++ {
		id := docs[i].ID % shards
		buf[id] = append(buf[id], docs[i])
		//log.Printf("keys:%s", docs[i].Text)

		if len(buf[id]) > 20 {
			idxes[id].Add(buf[id])
			buf[id] = make([]index.Document, 0)
		}
	}

	for i := 0; i < len(buf); i++ {
		if len(buf[i]) > 0 {
			idxes[i].Add(buf[i])
		}
	}

	for i := 0; i < shards; i++ {
		idxes[i].BT.Drain()
		log.Printf("sharding index_%d has %d keys", i, idxes[i].BT.Count())
		idxes[i].Close()
	}
	log.Printf("build index %d documents in %v", len(docs), time.Since(start))
}
