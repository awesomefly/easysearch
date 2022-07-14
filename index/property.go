package index

type DataRange struct {
	Start int
	End   int
}

type Property struct {
	// docNum is the count of documents
	docNum int

	// tokenCount is the total length of tokens
	tokenCount int

	//dataRange
	dataRange DataRange
}

func (idx *Property) DocNum() int {
	return idx.docNum
}

func (idx *Property) SetDocNum(num int) {
	idx.docNum = num
}

func (idx *Property) TokenCount() int {
	return idx.tokenCount
}

func (idx *Property) SetTokenCount(cnt int) {
	idx.tokenCount = cnt
}

func (idx *Property) DataRange() DataRange {
	return idx.dataRange
}

func (idx *Property) SetDataRange(d DataRange)  {
	idx.dataRange = d
}
