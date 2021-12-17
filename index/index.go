package index

type Index interface {
	Add(docs []Document)
	Retrieval(must []string, should []string, not []string, k int, r int) []Doc
}
