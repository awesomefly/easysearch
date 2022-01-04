package index

import (
	"log"
	"math"
	"sort"
)

type TF map[string]int32
type TFIDF struct {
	IDF map[string]float64
	DOC map[int32]TF
}

func NewTFIDF() *TFIDF {
	return &TFIDF{
		IDF: make(map[string]float64),
		DOC: make(map[int32]TF, 0),
	}
}

func CalIDF(docNum int, df int) float64 {
	return math.Log2(float64(docNum)/float64(df) + 1)
}

//CalCosine 余弦距离相似度 https://blog.csdn.net/weixin_42398658/article/details/85063004
func CalCosine(hits []Doc, tfidf *TFIDF) []Doc {

	queryDocId := int32(-1)

	var querySum float64
	for term, tf := range tfidf.DOC[queryDocId] {
		idf := tfidf.IDF[term]
		weight := float64(tf) * idf
		querySum += math.Pow(weight, 2)
	}

	for i, hit := range hits {
		var docSum, multiplySum float64
		for term, tf := range tfidf.DOC[hit.ID] {
			idf := tfidf.IDF[term]
			docTermWeight := float64(tf) * idf
			queryTermWeight := float64(tfidf.DOC[queryDocId][term]) * idf

			multiplySum += docTermWeight * queryTermWeight
			docSum += math.Pow(docTermWeight, 2)
		}
		hits[i].Cosine = multiplySum / math.Sqrt(querySum * docSum)
	}
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].Cosine > hits[j].Cosine //降序
	})
	log.Printf("Cosine sorted:%+v", hits)
	return hits
}

//CalBM25 计算bm25得分并排序
//docsLen 索引文档总长度, DocsNum 索取文档总数
func CalBM25(hits []Doc, tfidf *TFIDF, docLen int, docNum int) []Doc {
	// 计算bm25 参考:https://www.jianshu.com/p/1e498888f505
	for i, hit := range hits {
		for term, tf := range tfidf.DOC[hit.ID] { //hit doc包含多个term
			d := float64(docLen)
			avg := float64(docLen) / float64(docNum)
			idf := tfidf.IDF[term]
			k1 := float64(2)
			b := 0.75
			hits[i].BM25 += idf * float64(tf) * (k1 + 1) / (float64(tf) + k1*(1-b+b*d/avg))
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].BM25 > hits[j].BM25 //降序
	})
	log.Printf("bm25 sorted:%+v", hits)
	return hits
}