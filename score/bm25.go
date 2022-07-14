package score

import (
	"github.com/go-nlp/bm25"
	"github.com/go-nlp/tfidf"
	"sort"
)

type BM25Document []int           //token id list
func (d BM25Document) IDs() []int { return []int(d) }

func newTFIDF(docs []BM25Document) *tfidf.TFIDF {
	tf := tfidf.New()

	for _, doc := range docs {
		tf.Add(doc)
	}
	tf.CalculateIDF()
	return tf
}

// MostSimilar 相关性计算
// q query words, docs is doc id list, return most similar docs' id list
func MostSimilar(docCorpus map[int]BM25Document, tokenCorpus map[string]int, q []string, docs []int, k int) []int {
	// sort by bm25
	// 相关性评分请先阅读：https://www.jianshu.com/p/1e498888f505
	// 废弃-词集过大时，docs无法完全放入内存，需要自行统计词频并计算score
	var corpus []BM25Document
	for _, ts := range docCorpus {
		corpus = append(corpus, ts)
	}
	tf := newTFIDF(corpus)

	var query BM25Document
	for _, term := range q {
		query = append(query, tokenCorpus[term])
	}

	resultDocs := make([]tfidf.Document, 0, len(docs))
	for _, id := range docs {
		resultDocs = append(resultDocs, docCorpus[id])
	}

	// FIXME: IDF计算公式不对
	scores := bm25.BM25(tf, query, resultDocs, 2, 0.75)
	sort.Sort(scores) //order by asc
	//sort.Reverse(scores) //order by desc

	var final []int
	for i := len(scores) - 1; i >= 0 && k > 0; i-- {
		final = append(final, docs[scores[i].ID])
		k--
	}
	return final
}
