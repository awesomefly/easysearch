package search

import (
	"github.com/awesomefly/easysearch/config"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestIndexer(t *testing.T) {
	reg,_ := regexp.Compile("enwiki-latest-abstract.*.xml.gz")
	files,_ := Walk("../data", reg)
	println(files)
}

func TestIndex(t *testing.T) {
	return
	conf := config.Config {
		Store:config.Storage {
			DumpFile:  "../data/enwiki-latest-abstract18.xml.gz",
			IndexFile: "../data/enwiki_idx",
		},
	}


	Index(conf)
	r := recover()
	assert.Nil(t, r)
}