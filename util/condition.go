package util

func IfElseInt(condition bool, o1 int, o2 int) int {
	if condition {
		return o1
	}
	return o2
}
