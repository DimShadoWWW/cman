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
