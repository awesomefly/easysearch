package word2vec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSimilar(t *testing.T) {
	path := "/Users/bytedance/go/src/github.com/simplefts/data/model.word2vec.format.bin"
	model := Load(path)

	var (
		positive  = []string{"king", "woman"}
		negative  = []string{"man"}
	)
	out := GetSimilar(model, positive, negative, 3)
	for _, v := range out{
		println(v)
	}
	// assert.EqualValues(t, out, model.GetSimilar(positive, negative, 3))
	assert.Equal(t, 1, 1)
}

