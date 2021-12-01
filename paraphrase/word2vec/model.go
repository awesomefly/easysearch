package word2vec

// https://pkg.go.dev/code.sajari.com/word2vec#section-readme
//https://github.com/sajari/word2vec
import (
	"log"
	"os"

	"code.sajari.com/word2vec"
)

func Load(path string) *word2vec.Model {
	// Load the model from an io.Reader (i.e. a file).
	file, err := os.Open(path)
	defer file.Close()

	if err != nil {
		log.Fatalf("error open file fail: %v", err)
		panic(err)
	}
	//r := bufio.NewReader(file)

	model, err := word2vec.FromReader(file)
	if err != nil {
		log.Fatalf("error loading model: %v", err)
		panic(err)
	}
	return model
}

//GetSimilar 语义改写、近义词
func GetSimilar(model *word2vec.Model, positive []string, negative []string, n int) []string {
	// Create an expression.
	expr := word2vec.Expr{}
	for _, text := range positive {
		expr.Add(1, text)
	}
	for _, text := range negative {
		expr.Add(-1, text)
	}

	// Find the most similar result by cosine similarity.
	matches, err := model.CosN(expr, n)
	if err != nil {
		log.Fatalf("error evaluating cosine similarity: %v", err)
	}

	var result []string
	for _, match := range matches {
		result = append(result, match.Word)
	}
	return result
}
