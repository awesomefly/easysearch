package main

import (
	"github.com/go-nlp/bm25"
	"github.com/go-nlp/tfidf"
	"github.com/xtgo/set"
	"log"
	"sort"
)


type PostingItem struct {
	ID    int		//doc id
	Score float64  //bm25 score
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

// add adds documents to the index.
// todo: Store the index on disk and Support indexing multiple document fields.
//
//Sort results by relevance.
func (idx Index) add(docs []document) {
	var tokenId int
	tokenCorpus := make(map[string]int)
	docCorpus := make(map[int]BM25Document)

	for _, doc := range docs {
		var ts []int
		for _, token := range analyze(doc.Text) {
			//tfidf doc's token id list
			if _, ok := tokenCorpus[token]; !ok {
				tokenCorpus[token] = tokenId
				tokenId++
			}
			ts = append(ts, tokenCorpus[token])

			//add to postinglist
			postingList := idx[token]
			if postingList != nil && postingList[len(postingList)-1].ID == doc.ID {
				// Don't add same ID twice.
				continue
			}
			item := PostingItem{
				ID : doc.ID,
			}
			idx[token] = append(postingList, item)
		}
		docCorpus[doc.ID] = ts
	}

	//calculate bm25 score
	var docSlice []BM25Document
	for _, ts := range docCorpus {
		docSlice = append(docSlice, ts)
	}
	tf := NewTFIDF(docSlice)

	for k, v := range idx {
		_q := BM25Document{tokenCorpus[k]}
		_docs := make([]tfidf.Document, 0, len(v))
		for _, item := range v {
			_docs = append(_docs, docCorpus[item.ID])
		}
		_scores := bm25.BM25(tf, _q, _docs, 2, 0.75)
		for _, ds := range _scores {
			idx[k][ds.ID].Score = ds.Score
		}
	}
}

// inter returns the set intersection between a and b.
// a and b have to be sorted in ascending order and contain no duplicates.
func inter(a []int, b []int) []int {
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

// inter returns the set intersection between a and b that score accumulated.
// a and b have to be sorted in desc order by score and contain no duplicates.
func interPostingList(a PostingList, b PostingList) PostingList {
	docs := make([]int, 0, len(a)+len(b))
	docs = append(docs, a.IDs()...)
	docs = append(docs, b.IDs()...)
	size := set.Inter(sort.IntSlice(docs), len(a))

	result := make([]PostingItem, 0, size)
	for _, id := range docs[:size] {//interesction docs
		result = append(result, PostingItem {
			ID: id,
			Score: a.Find(id).Score + b.Find(id).Score,
		})
	}
	return result
}

func (idx Index) retrieval(text string, k int, r int) []int {
	var result PostingList
	for _, term := range analyze(text) {
		if pl, ok := idx[term]; ok {
			sort.Sort(pl)
			plr := pl[:IfElseInt(len(pl)>r, r, len(pl))]
			if result == nil {
				result = plr  //胜者表，截断r
			} else {
				//todo: Extend boolean queries to support OR and NOT.
				result = interPostingList(result, plr)
			}
		} else {
			// Token doesn't exist.
			return nil
		}
	}
	for _, item := range result {
		log.Printf("%d\t%f\n", item.ID, item.Score)
	}
	sort.Sort(result)
	for _, item := range result {
		log.Printf("after sort:%d\t%f\n", item.ID, item.Score)
	}

	ids := result.IDs()
	return ids[:IfElseInt(len(ids)>k, k, len(ids))]
}

// search queries the index for the given text.
// 相关性评分请先阅读：https://www.jianshu.com/p/1e498888f505
// retrieval(PageRank、bm25) -> 粗排sort(LR) -> 精排sort(DNN) -> topN(堆排序)
func (idx Index) search(text string) []int {
	r := idx.retrieval(text, 10, 1000)
	return r
}
