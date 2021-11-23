package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostingList(t *testing.T) {
	it := PostingItem{
		ID:        1,
		Frequency: 30,
		Score:     10.11,
	}

	it2 := PostingItem{
		ID:        2,
		Frequency: 20,
		Score:     20.22,
	}

	var pl PostingList
	pl = append(pl, it)
	pl = append(pl, it2)

	fmt.Printf("%+v\n", pl)

	bytes := pl.Bytes()
	pl2 := FromBytes(bytes)
	fmt.Printf("%+v\n", pl2)
	assert.Equal(t, len(pl), len(pl2))
}
