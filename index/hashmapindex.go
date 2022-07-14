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
	tbl map[string]PostingList

	property Property
}

func NewHashMapIndex() *HashMapIndex {
	return &HashMapIndex{
		tbl: make(map[string]PostingList),
		property: Property{
			docNum:     0,
			tokenCount: 0,
			dataRange:  DataRange{Start: 0, End: 0},
		},
	}
}

func (idx *HashMapIndex) Property() *Property {
	return &idx.property
}

func (idx *HashMapIndex) Map() map[string]PostingList {
	return idx.tbl
}

func (idx *HashMapIndex) Keys() []string {
	//keys := make(sort.StringSlice, idx.Property().tokenCount)
	var keys sort.StringSlice
	for k := range idx.tbl { //map 遍历访问是无序的
		keys = append(keys, k)
	}
	return keys
}
// Add adds documents to the index.
// todo: Support indexing multiple document fields.
func (idx *HashMapIndex) Add(docs []Document) {
	for _, doc := range docs {
		tokens := util.Analyze(doc.Text)
		for _, token := range tokens {
			postingList := idx.tbl[token]
			if postingList != nil {
				if last := postingList.Find(doc.ID); last != nil {
					// Don't add same ID twice. But should update frequency
					//last := &postingList[tokenCount(postingList)-1]
					last.TF++
					last.QualityScore = CalDocScore(last.TF, 0)
					//idx.tbl[token] = postingList
					continue
				}
			}
			item := Doc{
				ID:           int32(doc.ID),
				DocLen:       int32(len(tokens)),
				TF:           1,
				QualityScore: CalDocScore(1, 0),
			}
			//add to posting list
			idx.tbl[token] = append(postingList, item)
		}

		idx.property.docNum++
		idx.property.tokenCount += len(tokens)
	}

	//sort by score
	for k, v := range idx.tbl {
		sort.Slice(v, func(i, j int) bool {
			return v[i].QualityScore > v[j].QualityScore
		})
		idx.tbl[k] = v
	}
}

// Clear unsafe function
func (idx *HashMapIndex) Clear() {
	idx.property.docNum = 0
	idx.property.tokenCount = 0
	idx.property.dataRange = DataRange{Start: 0, End: 0}
	idx.tbl = make(map[string]PostingList)
}

func (idx *HashMapIndex) Get(term string) []Doc {
	if postingList, ok := idx.tbl[term]; ok {
		return postingList
	}
	return nil
}

func (idx *HashMapIndex) Retrieval(must []string, should []string, not []string, k int, r int, m SearchModel) []Doc {
	return DoRetrieval(idx, must, should, not, k, r, m)
}