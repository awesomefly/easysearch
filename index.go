package main

import (
	"github.com/chenquan1988/simplefts/common"
	"github.com/chenquan1988/simplefts/score"
	"github.com/go-nlp/bm25"
	"github.com/go-nlp/tfidf"
	"github.com/xtgo/set"
	"sort"
)


type PostingItem struct {
	ID    int		//doc id
	Frequency int	//词频
	Score float64   //静态分
}

type PostingList []PostingItem
func (pl PostingList) Len() int{ return len(pl) }
func (pl PostingList) Less(i, j int) bool { return pl[i].Score > pl[j].Score } //降序
func (pl PostingList) Swap(i, j int) {
	pl[i].Score, pl[j].Score = pl[j].Score, pl[i].Score
	pl[i].ID, pl[j].ID = pl[j].ID, pl[i].ID
}
func (pl PostingList) Find(id int) *PostingItem {
	for _, item := range pl {
		if item.ID == id {
			return &item
		}
	}
	return nil
}
func (pl PostingList) IDs() []int {
	ids:= make([]int, 0, len(pl))
	for _, item := range pl {
		ids = append(ids, item.ID)
	}
	sort.Sort(sort.IntSlice(ids))
	return ids
}


// Index is an inverted index. It maps tokens to document IDs.
type Index map[string]PostingList

func IfElseInt(condition bool, o1 int, o2 int) int {
	if condition {
		return o1
	}
	return o2
}

// CalDocScore todo: 拟合函数计算文档分
func CalDocScore(frequency int, pagerank int) float64 {
	return float64(frequency * 1.0)
}

var tokenCorpus = make(map[string]int)
var docCorpus = make(map[int]score.BM25Document)

// Add adds documents to the index.
// todo: Store the index on disk and Support indexing multiple document fields.
//Sort results by relevance.
func (idx Index) Add(docs []document) {
	var tokenId int
	for _, doc := range docs {
		var ts []int
		for _, token := range common.analyze(doc.Text) {
			//tfidf doc's token id list
			if _, ok := tokenCorpus[token]; !ok {
				tokenCorpus[token] = tokenId
				tokenId++
			}
			ts = append(ts, tokenCorpus[token])

			postingList := idx[token]
			if postingList != nil {
				if last := &postingList[len(postingList)-1]; last.ID == doc.ID {
					// Don't add same ID twice. But should update frequency
					last := &postingList[len(postingList)-1]
					last.Frequency++
					last.Score = CalDocScore(last.Frequency, 0)
					continue
				}
			}
			item := PostingItem{
				ID : doc.ID,
				Frequency: 1,
				Score: CalDocScore(1, 0), //todo: calculate doc static score by PageRank
			}
			//add to posting list
			idx[token] = append(postingList, item)
		}
		docCorpus[doc.ID] = ts
	}

	//sort by score
	for _, v := range idx {
		sort.Sort(v)
	}
}

// InterInt returns the set intersection between a and b.
// a and b have to be sorted in ascending order and contain no duplicates.
func InterInt(a []int, b []int) []int {
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	r := make([]int, 0, maxLen)
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			r = append(r, a[i])
			i++
			j++
		}
	}
	return r
}

// Inter returns the set intersection between a and b that score accumulated.
// a and b have to be sorted in desc order by score and contain no duplicates.
func Inter(a PostingList, b PostingList) PostingList {
	docs := make([]int, 0, len(a)+len(b))
	docs = append(docs, a.IDs()...)
	docs = append(docs, b.IDs()...)
	size := set.Inter(sort.IntSlice(docs), len(a))

	result := make([]PostingItem, 0, size)
	for _, id := range docs[:size] {//interesction docs
		result = append(result, PostingItem {
			ID: id,
		})
	}
	return result
}

func (idx Index) retrieval(text string, k int, r int) []int {
	var result []int
	terms := common.analyze(text)
	for _, term := range terms {
		if pl, ok := idx[term]; ok {
			plr := pl[:IfElseInt(len(pl)>r, r, len(pl))]
			if result == nil {
				result = plr.IDs()  //胜者表，截断r
			} else {
				//todo: Extend boolean queries to support OR and NOT.
				result = InterInt(result, plr.IDs())
			}
		} else {
			// Token doesn't exist.
			return nil
		}
	}

	if len(result) == 0 {
		return result
	}

	// sort by bm25
	// 相关性评分请先阅读：https://www.jianshu.com/p/1e498888f505
	var docs []score.BM25Document
	for _, ts := range docCorpus {
		docs = append(docs, ts)
	}
	tf := score.NewTFIDF(docs)

	var query score.BM25Document
	for _, term := range terms {
		query = append(query, tokenCorpus[term])
	}

	resultDocs := make([]tfidf.Document, 0, len(result))
	for _, id := range result {
		resultDocs = append(resultDocs, docCorpus[id])
	}

	scores := bm25.BM25(tf, query, resultDocs, 2, 0.75)
	sort.Sort(scores) //order by asc

	var final []int
	for i := len(scores)-1; i >= 0 && k > 0; i--{
		final = append(final, result[scores[i].ID])
		k--
	}
	return final
}

// search queries the index for the given text.
// todo: retrieval(pagerank+frequency) -> sort(bm25) -> 粗排sort(CTR by LR) -> 精排sort(CVR by DNN) -> topN(堆排序)
func (idx Index) search(text string) []int {
	// todo: 搜索词改写、向量检索
	r := idx.retrieval(text, 10, 1000)
	return r
}
