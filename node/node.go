package node

import (
	"fmt"
	"strings"
)

type Node struct {
	Port     int
	Priority int
	Host     string
	Key      string
}

func (n Node) Split() []string {
	result := strings.Split(n.Host, ".")
	result = append(result, string(n.Port))
	return result
}
func (n Node) Less(o Node) bool {
	ir := n.Split()
	jr := o.Split()
	for k, _ := range ir {
		if ir[k] < jr[k] {
			return true
		}
	}
	return false
}
func (n Node) Equal(o Node) bool {
	ir := n.Split()
	jr := o.Split()
	for k, _ := range ir {
		if ir[k] < jr[k] || ir[k] > jr[k] {
			return false
		}
	}
	return true
}
func (n Node) Compare(o Node) int {
	ir := n.Split()
	jr := o.Split()
	for k, _ := range ir {
		if ir[k] < jr[k] {
			return -1
		} else if ir[k] > jr[k] {
			return 1
		}
	}
	return 0
}

func (n Node) String() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}
