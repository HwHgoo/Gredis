package utils

const (
	fnv32_prime = uint32(16777619)
	fnv32_basis = uint32(2166136261)
)

// FNV-1a hash function
func Fnv32(key []byte) uint32 {
	hash := fnv32_basis
	for _, b := range key {
		hash ^= uint32(b)
		hash *= fnv32_prime
	}

	return hash
}
