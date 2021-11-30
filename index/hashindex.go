package index

import (
	"sort"

	"github.com/awesomefly/simplefts/score"

	"github.com/xtgo/set"

	"github.com/awesomefly/simplefts/common"
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
// todo: calculate doc static score by PageRank + frequency
func CalDocScore(frequency int32, pagerank int) float64 {
	return float64(frequency * 1.0)
}

var TokenCorpus = make(map[string]int)
var DocCorpus = make(map[int]score.BM25Document)

// Add adds documents to the index.
// todo: Support indexing multiple document fields.
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
				if last := &postingList[len(postingList)-1]; last.ID == int32(doc.ID) {
					// Don't add same ID twice. But should update frequency
					//last := &postingList[len(postingList)-1]
					last.TF++
					last.Score = CalDocScore(last.TF, 0)
					continue
				}
			}
			item := Doc{
				ID:    int32(doc.ID),
				TF:    1,
				Score: CalDocScore(1, 0),
			}
			//add to posting list
			idx[token] = append(postingList, item)
		}
		DocCorpus[doc.ID] = ts
	}

	//sort by score
	for k, v := range idx {
		sort.Slice(v, func(i, j int) bool {
			return v[i].Score > v[j].Score
		})
		idx[k] = v
	}
}

// Retrieval returns top k docs sorted by bm25
// todo: compress posting list and opt intersection/union rt
// https://blog.csdn.net/weixin_39890629/article/details/111268898
func (idx HashIndex) Retrieval(must []string, should []string, not []string, k int, r int) []Doc {
	var result PostingList
	for _, term := range must {
		if pl, ok := idx[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按TF排序,截断前r个,加速归并
			sort.Sort(plr)

			if result == nil {
				result = plr
			} else {
				l := len(result)
				result = append(result, plr...)
				size := set.Inter(result, l)
				result = result[:size]
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
				result = plr //胜者表，截断r
			} else {
				l := len(result)
				result = append(result, plr...)
				size := set.Union(result, l)
				result = result[:size]
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl, ok := idx[term]; ok {
			sort.Sort(pl)

			l := len(result)
			tmp := append(result, pl...)
			size := set.Inter(tmp, l)
			inter := tmp[:size]

			result = append(result, inter...)
			size = set.Diff(result, l)
			result = result[:size]
		} else {
			// Token doesn't exist.
			continue
		}
	}

	//相关性排序，待优化
	//score.MostSimilar(DocCorpus, TokenCorpus, must, r, 10)

	if len(result) > k {
		return result[:k]
	}
	return result
}