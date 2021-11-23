package index

import (
	"fmt"
	"os"
	"testing"

	"github.com/awesomefly/simplefts/common"

	"github.com/awesomefly/simplefts/store"
	"github.com/stretchr/testify/assert"
)

func TestBTreeIndex(t *testing.T) {
	os.Remove("/Users/bytedance/go/src/github.com/simplefts/data/test_insread_index.dat")
	os.Remove("/Users/bytedance/go/src/github.com/simplefts/data/test_insread_kv.dat")

	idx := NewBTreeIndex()
	idx.Add([]store.Document{{ID: 1, Text: "A donut on a glass plate. Only the donuts."}})

	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10))
	assert.Equal(t, []int{1}, idx.Retrieval([]string{"donut"}, nil, nil, 100, 10))
	assert.Equal(t, []int{1}, idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10))
	assert.Equal(t, []int{1}, idx.Retrieval([]string{"glass"}, nil, nil, 100, 10))

	idx.Add([]store.Document{{ID: 2, Text: "donut is a donut"}})
	fmt.Printf("%+v", idx.Get("donut"))
	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10))
	assert.Equal(t, []int{2, 1}, idx.Retrieval([]string{"donut"}, nil, nil, 100, 10))
	assert.Equal(t, []int{2, 1}, idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10))
	assert.Equal(t, []int{1}, idx.Retrieval([]string{"glass"}, nil, nil, 100, 10))
}
