package gangliamr

// Return the first non empty string, or if all are empty, the empty string.
func nonEmpty(s ...string) string {
	for _, e := range s {
		if e != "" {
			return e
		}
	}
	return ""
}

// Prepends base if non empty.
func makeOptional(base, extra string) string {
	if base == "" {
		return ""
	}
	return base + " " + extra
}
