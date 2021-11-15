package score

import (
	"github.com/go-nlp/tfidf"
)

type BM25Document []int           //token id list
func (d BM25Document) IDs() []int { return []int(d) }

func NewTFIDF(docs []BM25Document) *tfidf.TFIDF {
	tf := tfidf.New()

	for _, doc := range docs {
		tf.Add(doc)
	}
	tf.CalculateIDF()
	return tf
}