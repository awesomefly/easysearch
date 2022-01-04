package index

type Index interface {
	Add(docs []Document)
	Get(terms []string, r int) map[Term][]Doc

	//BooleanRetrieval by boolean model
	BooleanRetrieval(must []string, should []string, not []string, k int, r int) []Doc

	//VecSpaceRetrieval by vector space model
	VecSpaceRetrieval(terms []string, k int, r int) []Doc

	//ProbRetrieval by probabilistic model 概率模型检索
	ProbRetrieval(terms []string, k int, r int) []Doc
}
