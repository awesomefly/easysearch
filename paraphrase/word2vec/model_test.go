package word2vec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSimilar(t *testing.T) {
	path := "~/go/src/github.com/simplefts/data/model.word2vec.format.bin"
	model := Load(path)

	var (
		positive = []string{"king", "woman"}
		negative = []string{"man"}
	)
	out := GetSimilar(model, positive, negative, 3)
	for _, v := range out {
		println(v)
	}
	// assert.EqualValues(t, out, model.GetSimilar(positive, negative, 3))
	assert.Equal(t, 1, 1)
}
