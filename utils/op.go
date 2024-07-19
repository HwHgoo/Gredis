package utils

func TerneryOp[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}
