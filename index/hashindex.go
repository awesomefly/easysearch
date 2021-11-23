package index

import (
	"sort"

	"github.com/awesomefly/simplefts/common"
	"github.com/awesomefly/simplefts/score"
	"github.com/awesomefly/simplefts/store"
)

// HashIndex is an inverted index. It maps tokens to document IDs.
type HashIndex map[string]PostingList

func IfElseInt(condition bool, o1 int, o2 int) int {
	if condition {
		return o1
	}
	return o2
}

// CalDocScore
// todo: 拟合函数计算文档分
func CalDocScore(frequency int, pagerank int) float64 {
	return float64(frequency * 1.0)
}

// Add adds documents to the index.
// todo: Store the index on disk and Support indexing multiple document fields.
// todo: Support indexing big dictionary (hashmap & b+ tree)
// todo: Support big corpus full indexing, realtime increment update/delete (double buffer + delete doc list)
// todo: Support distributed
func (idx HashIndex) Add(docs []store.Document) {
	var tokenID int
	for _, doc := range docs {
		var ts []int
		for _, token := range common.Analyze(doc.Text) {
			//tfidf doc's token id list
			if _, ok := TokenCorpus[token]; !ok {
				TokenCorpus[token] = tokenID
				tokenID++
			}
			ts = append(ts, TokenCorpus[token])

			postingList := idx[token]
			if postingList != nil {
				if last := &postingList[len(postingList)-1]; last.ID == doc.ID {
					// Don't add same ID twice. But should update frequency
					//last := &postingList[len(postingList)-1]
					last.Frequency++
					last.Score = CalDocScore(last.Frequency, 0)
					continue
				}
			}
			item := PostingItem{
				ID:        doc.ID,
				Frequency: 1,
				Score:     CalDocScore(1, 0), //todo: calculate doc static score by PageRank
			}
			//add to posting list
			idx[token] = append(postingList, item)
		}
		DocCorpus[doc.ID] = ts
	}

	//sort by score
	for k, v := range idx {
		sort.Sort(v)
		idx[k] = v
	}
}

// Retrieval returns top k docs sorted by bm25
// todo: compress posting list and opt intersection/union rt
// https://blog.csdn.net/weixin_39890629/article/details/111268898
func (idx HashIndex) Retrieval(must []string, should []string, not []string, k int, r int) []int {
	var result []int
	for _, term := range must {
		if pl, ok := idx[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按frequency排序,截断前r个,加速归并
			if result == nil {
				result = plr.IDs()
			} else {
				result = common.InterInt(result, plr.IDs())
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		if pl, ok := idx[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			if result == nil {
				result = plr.IDs() //胜者表，截断r
			} else {
				result = common.MergeInt(result, plr.IDs())
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl, ok := idx[term]; ok {
			result = common.FilterInt(result, pl.IDs())
		} else {
			// Token doesn't exist.
			continue
		}
	}

	if len(result) == 0 {
		return result
	}

	return score.MostSimilar(DocCorpus, TokenCorpus, must, result, k)
}
