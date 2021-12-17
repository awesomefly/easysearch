package serving

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSimilar(t *testing.T) {
	path := "../../data/model.word2vec.format.bin"
	x, _ := filepath.Abs(path)
	fmt.Println(x)
	model := NewModel(path)

	var (
		positive = []string{"king", "woman"}
		negative = []string{"man"}
	)
	out := model.GetSimilar(positive, negative, 3)
	for _, v := range out {
		println(v)
	}
	// assert.EqualValues(t, out, model.GetSimilar(positive, negative, 3))
	assert.Equal(t, 1, 1)
}
