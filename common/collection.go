package common

// InterInt returns the set intersection between a and b.
// a and b have to be sorted in ascending order and contain no duplicates.
func InterInt(a []int, b []int) []int {
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	r := make([]int, 0, maxLen)
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			r = append(r, a[i])
			i++
			j++
		}
	}
	return r
}

// MergeInt returns the unique set a union b.
// a and b have to be sorted in ascending order and contain no duplicates.
func MergeInt(a []int, b []int) []int {
	r := make([]int, 0, len(a)+len(b))
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			r = append(r, a[i])
			i++
		} else if a[i] > b[j] {
			r = append(r, b[j])
			j++
		} else {
			r = append(r, a[i])
			i++
			j++
		}
	}
	return r
}

// DiffInt returns the diff set a between b.
// a and b have to be sorted in ascending order and contain no duplicates.
func DiffInt(a []int, b []int) []int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	r := make([]int, 0, minLen)
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			r = append(r, a[i])
			i++
		} else if a[i] > b[j] {
			r = append(r, b[j])
			j++
		} else {
			i++
			j++
		}
	}
	return r
}

// FilterInt returns the set a filter b.
// a and b have to be sorted in ascending order and contain no duplicates.
func FilterInt(a []int, b []int) []int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	r := make([]int, 0, minLen)
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			r = append(r, a[i])
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			i++
			j++
		}
	}
	return r
}
