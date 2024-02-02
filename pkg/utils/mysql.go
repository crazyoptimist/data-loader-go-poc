package utils

// Generates a query placeholder string
// Parameters:
//
//	n: number of query arguments
//
// Returns:
//
//	query placeholder string
func GenerateMYSQLPlaceholder(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}
