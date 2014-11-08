package node

type NodeList []string

// Implement the three functions from sort.Interface (part of permutation.Sequence interface)
func (p NodeList) Len() int { return len(p) }
func (p NodeList) Less(i, j int) bool {
	ir := []byte(p[i])
	jr := []byte(p[j])
	for k, _ := range ir {
		if ir[k] < jr[k] {
			return true
		}
	}
	return false
}
func (p NodeList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p NodeList) Replace(indices []int) NodeList {
	result := make(NodeList, len(indices), len(indices))
	for i, idx := range indices {
		result[i] = p[idx]
	}
	return result
}

//Permutation generator
func Permutations(list NodeList, select_num int, buf int) (c chan NodeList) {
	c = make(chan NodeList, buf)
	go func() {
		defer close(c)
		var perm_generator func([]int, int, int) chan []int
		perm_generator = permutations
		indices := make([]int, list.Len(), list.Len())
		for i := 0; i < list.Len(); i++ {
			indices[i] = i
		}
		for perm := range perm_generator(indices, select_num, buf) {
			c <- list.Replace(perm)
		}
	}()
	return
}
func pop(l []int, i int) (v int, sl []int) {
	v = l[i]
	length := len(l)
	sl = make([]int, length-1, length-1)
	copy(sl, l[:i])
	copy(sl[i:], l[i+1:])
	return
}

//Permutation generator for int slice
func permutations(list []int, select_num, buf int) (c chan []int) {
	c = make(chan []int, buf)
	go func() {
		defer close(c)
		switch select_num {
		case 1:
			for _, v := range list {
				c <- []int{v}
			}
			return
		case 0:
			return
		case len(list):
			for i := 0; i < len(list); i++ {
				top, sub_list := pop(list, i)
				for perm := range permutations(sub_list, select_num-1, buf) {
					c <- append([]int{top}, perm...)
				}
			}
		default:
			for comb := range combinations(list, select_num, buf) {
				for perm := range permutations(comb, select_num, buf) {
					c <- perm
				}
			}
		}
	}()
	return
}

//Combination generator
func Combinations(list NodeList, select_num int, buf int) (c chan NodeList) {
	c = make(chan NodeList, buf)
	index := make([]int, list.Len(), list.Len())
	for i := 0; i < list.Len(); i++ {
		index[i] = i
	}
	var comb_generator func([]int, int, int) chan []int
	comb_generator = combinations
	go func() {
		defer close(c)
		for comb := range comb_generator(index, select_num, buf) {
			c <- list.Replace(comb)
		}
	}()
	return
}

//Combination generator for int slice
func combinations(list []int, select_num, buf int) (c chan []int) {
	c = make(chan []int, buf)
	go func() {
		defer close(c)
		switch {
		case select_num == 0:
			c <- []int{}
		case select_num == len(list):
			c <- list
		case len(list) < select_num:
			return
		default:
			for i := 0; i < len(list); i++ {
				for sub_comb := range combinations(list[i+1:], select_num-1, buf) {
					c <- append([]int{list[i]}, sub_comb...)
				}
			}
		}
	}()
	return
}
