package index

import (
	"testing"

	"github.com/awesomefly/simplefts/common"

	"github.com/awesomefly/simplefts/store"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	idx := make(HashIndex)

	idx.Add([]store.Document{{ID: 1, Text: "A donut on a glass plate. Only the donuts."}})
	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10))

	result := idx.Retrieval([]string{"donut"}, nil, nil, 100, 10)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	result = idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	result = idx.Retrieval([]string{"glass"}, nil, nil, 100, 10)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())

	idx.Add([]store.Document{{ID: 2, Text: "donut is a donut"}})
	assert.Nil(t, idx.Retrieval([]string{"a"}, nil, nil, 100, 10))

	result = idx.Retrieval([]string{"donut"}, nil, nil, 100, 10)
	assert.Equal(t, []int{1, 2}, (PostingList)(result).IDs())

	result = idx.Retrieval(common.Analyze("DoNuts"), nil, nil, 100, 10)
	assert.Equal(t, []int{1, 2}, (PostingList)(result).IDs())

	result = idx.Retrieval([]string{"glass"}, nil, nil, 100, 10)
	assert.Equal(t, []int{1}, (PostingList)(result).IDs())
}
