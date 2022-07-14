package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostingList(t *testing.T) {
	it := Doc{
		ID:           1,
		TF:           30,
		QualityScore: 10.11,
	}

	it2 := Doc{
		ID:           2,
		TF:           20,
		QualityScore: 20.22,
	}

	bb := it.Bytes()
	it3 := Doc{}
	it3.FromBytes(bb)
	fmt.Printf("it3: %+v\n", it3)

	var pl PostingList
	pl = append(pl, it)
	pl = append(pl, it2)

	fmt.Printf("pl:%+v\n", pl)

	bytes := pl.Bytes()

	var pl2 PostingList
	pl2.FromBytes(bytes)
	fmt.Printf("pl2:%+v\n", pl2)
	assert.Equal(t, len(pl), len(pl2))
}
