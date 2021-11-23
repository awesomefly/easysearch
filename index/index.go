package index

import (
	"github.com/awesomefly/simplefts/score"
	"github.com/awesomefly/simplefts/store"
)

type Index interface {
	Add(docs []store.Document)
	Retrieval(must []string, should []string, not []string, k int, r int) []int
}

var TokenCorpus = make(map[string]int)
var DocCorpus = make(map[int]score.BM25Document)
