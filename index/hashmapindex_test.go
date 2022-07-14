package index

import (
	"fmt"
	"testing"

	"github.com/awesomefly/easysearch/util"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	idx := NewHashMapIndex()

	idx.Add([]Document{{ID: 1, Text: "A donut on a glass plate. Only the donut"}})
	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10, Boolean))

	result := idx.Retrieval([]string{"donut"}, nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	result = idx.Retrieval(util.Analyze("DoNuts"), nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	result = idx.Retrieval([]string{"glass"}, nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	for s, list := range idx.tbl {
		fmt.Printf("%s:%v\n", s, list)
	}

	//=====================================================
	idx.Add([]Document{{ID: 2, Text: "donut is a donut"}})
	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10, Boolean))

	result = idx.Retrieval([]string{"donut"}, nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1, 2}, (PostingList)(result).IDs())

	result = idx.Retrieval(util.Analyze("DoNuts"), nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1, 2}, (PostingList)(result).IDs())

	result = idx.Retrieval([]string{"glass"}, nil, nil, 100, 10, Boolean)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	for s, list := range idx.tbl {
		fmt.Printf("%s:%v\n", s, list)
	}
}
