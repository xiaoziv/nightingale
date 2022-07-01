package common

func StringSetKeys(m map[string]struct{}) []string {
	lst := make([]string, 0, len(m))
	for k := range m {
		lst = append(lst, k)
	}
	return lst
}
