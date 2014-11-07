package node

type NodeSlice map[string]Node

func (p NodeSlice) HasKey(key string) bool {
	for k, _ := range p {
		if k == key {
			return true
		}
	}
	return false
}

// // Implement the three functions from sort.Interface (part of permutation.Sequence interface)
// func (p NodeSlice) Len() int { return len(p) }
// func (p NodeSlice) Less(i, j int) bool {
// 	ir := p[i].Split()
// 	jr := p[j].Split()
// 	for k, _ := range ir {
// 		if ir[k] < jr[k] {
// 			return true
// 		}
// 	}
// 	return false
// }
// func (p NodeSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// func (p NodeSlice) Replace(indices []int) NodeSlice {
// 	result := make(NodeSlice, len(indices), len(indices))
// 	for i, idx := range indices {
// 		result[i] = p[idx]
// 	}
// 	return result
// }
