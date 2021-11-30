package index

import (
	"github.com/awesomefly/simplefts/store"
)

type BM25Parameters struct {
	K1 float64
	B  float64
}

type Options struct {
	StoreFile string // Summary file
	BM25Parameters
}

type Index interface {
	Add(docs []store.Document)
	Retrieval(must []string, should []string, not []string, k int, r int) []Doc
}
