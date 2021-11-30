package index

import (
	"fmt"
	"testing"

	"github.com/awesomefly/simplefts/common"
	"github.com/stretchr/testify/assert"
)

func GetIDs(docs []Doc) []int {
	var ids []int
	for _, doc := range docs {
		ids = append(ids, int(doc.ID))
	}
	return ids
}
func TestBTreeIndex(t *testing.T) {
	//os.Remove("/Users/bytedance/go/src/github.com/simplefts/data/btree_idx_test.idx")
	//os.Remove("/Users/bytedance/go/src/github.com/simplefts/data/btree_idx_test.kv")

	idx := NewBTreeIndex("/Users/bytedance/go/src/github.com/simplefts/data/btree_idx_test")
	fmt.Printf("Count:%d\n", idx.BT.Count())
	ch := idx.BT.FullSet()
	for {
		k := <-ch
		d := <-ch
		v := <-ch
		if k == nil || d == nil || v == nil {
			break
		}
		//id, err := strconv.ParseInt(string(d), 10, 64)  // key's id
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Printf("id:%d\n", id)

		var nv PostingList
		nv.FromBytes(v)
		fmt.Printf("key:%s, val:%+v\n", k, nv)
	}

	//idx.Add([]store.Document{{ID: 1, Text: "A donut on a glass plate. Only the."}})
	//idx.Add([]store.Document{{ID: 2, Text: "donut is a donut"}})
	fmt.Printf("Lookup: %+v\n", idx.Lookup("donut", false))
	fmt.Printf("Retrieval: %+v\n", idx.Retrieval([]string{"glass"}, []string{"donut"}, nil, 100, 10))

	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10))

	ids := GetIDs(idx.Retrieval([]string{"donut"}, nil, nil, 100, 10))
	assert.Equal(t, []int{2, 1}, ids)
	assert.Equal(t, []int{2, 1}, GetIDs(idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10)))
	assert.Equal(t, []int{1}, GetIDs(idx.Retrieval([]string{"glass"}, nil, nil, 100, 10)))

	assert.Nil(t, GetIDs(idx.Retrieval([]string{"a"}, nil, nil, 100, 10)))
	assert.Equal(t, []int{2, 1}, GetIDs(idx.Retrieval([]string{"donut"}, nil, nil, 100, 10)))
	assert.Equal(t, []int{2, 1}, GetIDs(idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10)))
	assert.Equal(t, []int{1}, GetIDs(idx.Retrieval([]string{"glass"}, nil, nil, 100, 10)))

	idx.Close()
}
