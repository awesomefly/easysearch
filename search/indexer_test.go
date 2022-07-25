package search

import (
	"github.com/awesomefly/easysearch/config"
	"github.com/awesomefly/easysearch/index"
	"regexp"
	"runtime"
	"testing"
)

func TestIndexer(t *testing.T) {
	reg,_ := regexp.Compile("enwiki-latest-abstract.*.xml.gz")
	files,_ := Walk("../data", reg)
	println(files)
}

func TestIndex(t *testing.T) {
	runtime.GOMAXPROCS(2)
	conf := config.Config {
		Store:config.Storage {
			DumpFile:  "../data/enwiki-latest-abstract27.xml.gz",
			IndexFile: "../data/enwiki_idx",
		},
	}


	Index(conf)
	//r := recover()
	//assert.Nil(t, r)

	bt := index.NewBTreeIndex(conf.Store.IndexFile)
	bt.BT.Stats(true)
}