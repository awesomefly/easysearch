package index

import (
	"github.com/awesomefly/easysearch/util"
	"sort"
)



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

// HashMapIndex is an inverted index. It maps tokens to document IDs.
type HashMapIndex struct {
	Map map[string]PostingList

	// DocNum is the count of documents
	DocNum int

	// Len is the total length of docs
	Len int
}

func NewHashMapIndex() *HashMapIndex {
	return &HashMapIndex{
		Map: make(map[string]PostingList),
		DocNum: 0,
		Len: 0,
	}
}

// Add adds documents to the index.
// todo: Support indexing multiple document fields.
func (idx HashMapIndex) Add(docs []Document) {
	for _, doc := range docs {
		tokens := util.Analyze(doc.Text)
		for _, token := range tokens {
			postingList := idx.Map[token]
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
				DocLen: int32(len(tokens)),
				TF:    1,
				Score: CalDocScore(1, 0),
			}
			//add to posting list
			idx.Map[token] = append(postingList, item)
		}

		idx.DocNum++
		idx.Len += len(tokens) // todo: adding only unique words
	}

	//sort by score
	for k, v := range idx.Map {
		sort.Slice(v, func(i, j int) bool {
			return v[i].Score > v[j].Score
		})
		idx.Map[k] = v
	}
}

func (idx HashMapIndex) Get(terms []string, r int) map[Term][]Doc {
	result := make(map[Term][]Doc, len(terms))
	for _, term := range terms {
		if postingList, ok := idx.Map[term]; ok {
			l := postingList[:IfElseInt(len(postingList) > r, r, len(postingList))] //胜者表按TF排序,截断前r个,加速归并
			result[Term{K: term, DF: int32(len(postingList))}] = l
		} else {
			result[Term{K: term}] = nil
		}
	}
	return result
}

// BooleanRetrieval returns top k docs sorted by boolean model
// todo: compress posting list and opt intersection/union rt
// https://blog.csdn.net/weixin_39890629/article/details/111268898
func (idx HashMapIndex) BooleanRetrieval(must []string, should []string, not []string, k int, r int) []Doc {
	var result PostingList
	for _, term := range must {
		if pl, ok := idx.Map[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按TF排序,截断前r个,加速归并
			sort.Sort(plr)
			if result == nil {
				result = plr
			} else {
				result.Inter(plr)
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		if pl, ok := idx.Map[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			sort.Sort(plr)
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl, ok := idx.Map[term]; ok {
			sort.Sort(pl)
			result.Filter(pl)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	if len(result) > k {
		return result[:k]
	}
	return result
}

//VecSpaceRetrieval by vector space model
func (idx HashMapIndex) VecSpaceRetrieval(terms []string, k int, r int) []Doc {
	tfidf := NewTFIDF()

	//query's term frequency
	queryDocId := int32(-1)
	tfidf.DOC[queryDocId] = make(TF, 0)

	var result PostingList
	for _, term := range terms {
		tfidf.DOC[queryDocId][term]++
		if pl, ok := idx.Map[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			//sort.Sort(plr)  //Union中会先sort在diff
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}

			tfidf.IDF[term] = CalIDF(idx.DocNum, len(pl))
			for _, doc := range plr {
				tf := tfidf.DOC[doc.ID]
				if tf == nil {
					tf = make(TF, 0)
				}
				tf[term] = doc.TF
				tfidf.DOC[doc.ID] = tf
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}

	result = CalCosine(result, tfidf)
	if len(result) > k {
		return result[:k]
	}
	return result
}

//ProbRetrieval by probabilistic model 概率模型检索
func (idx HashMapIndex) ProbRetrieval(terms []string, k int, r int) []Doc {
	tfidf := NewTFIDF()

	var result PostingList
	for _, term := range terms {
		if pl, ok := idx.Map[term]; ok {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			sort.Sort(plr)
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}
			tfidf.IDF[term] = CalIDF(idx.DocNum, len(pl))
			for _, doc := range plr {
				tf := tfidf.DOC[doc.ID]
				if tf == nil {
					tf = make(TF, 0)
				}
				tf[term] = doc.TF
				tfidf.DOC[doc.ID] = tf
			}
		} else {
			// Token doesn't exist.
			continue
		}
	}
	result = CalBM25(result, tfidf, idx.Len, idx.DocNum)
	if len(result) > k {
		return result[:k]
	}
	return result

}