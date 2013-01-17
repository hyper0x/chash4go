package chash4go

import (
	"sort"
)

type Node struct {
	Key    uint32
	Target string
}

/* 
 * A non-thread-safe ordered ring-like set
 */
type NodeRing []Node

func (self NodeRing) Len() int {
	return len(self)
}

func (self NodeRing) Less(i, j int) bool {
	return self[i].Key < self[j].Key
}

func (self NodeRing) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self NodeRing) Get(nodeKey uint32) *Node {
	length := len(self)
	if length == 0 {
		return nil
	}
	index := sort.Search(length, func(i int) bool { return self[i].Key == nodeKey })
	if index < length {
		matchedNode := self[index]
		return &Node{matchedNode.Key, matchedNode.Target}
	}
	return nil
}

func (self NodeRing) Next(nodeKey uint32) *Node {
	length := len(self)
	if length == 0 {
		return nil
	}
	index := sort.Search(length, func(i int) bool { return self[i].Key >= nodeKey })
	var matchedNode Node
	if index >= length {
		matchedNode = self[0]
	} else {
		matchedNode = self[index]
	}
	return &Node{matchedNode.Key, matchedNode.Target}
}

func (self NodeRing) Add(nodes ...Node) bool {
	paramLength := len(nodes)
	if paramLength == 0 {
		return false
	}
	selfLength := len(self)
	if selfLength == 0 {
		self = append(self, nodes...)
		return true
	}
	newNodes := make([]Node, paramLength)
	newNodeIndex := 0
	for i := 0; i < paramLength; i++ {
		node := nodes[i]
		index := sort.Search(selfLength, func(i int) bool { return self[i].Key == node.Key })
		if index >= selfLength {
			newNodes[newNodeIndex] = node
			newNodeIndex++
		} else {
			self[index].Target = node.Target
		}
	}
	if newNodeIndex == 0 {
		return false
	}
	newNodes = newNodes[:newNodeIndex+1]
	self = append(self, newNodes...)
	sort.Sort(self)
	return true
}

func (self NodeRing) Remove(nodeKey uint32) bool {
	length := len(self)
	if length == 0 {
		return false
	}
	index := sort.Search(length, func(i int) bool { return self[i].Key == nodeKey })
	if index < length {
		copy(self[index:], self[index+1:])
		// self[length-1] = Node{}
		self = self[:length-1]
		return true
	}
	return false
}

/* 
 * A non-thread-safe inserting-order set
 */
type NodeKeySet []uint32

func (self NodeKeySet) IndexOf(nodeKey uint32) int {
	index := sort.Search(len(self), func(i int) bool { return self[i] == nodeKey })
	if index >= len(self) {
		return -1
	}
	return index
}

func (self NodeKeySet) Add(nodeKeys ...uint32) bool {
	paramLength := len(nodeKeys)
	if paramLength == 0 {
		return false
	}
	selfLength := len(self)
	if selfLength == 0 {
		self = append(self, nodeKeys...)
		return true
	}
	newNodeKeys := make([]uint32, paramLength)
	newNodeKeyIndex := 0
	for i := 0; i < paramLength; i++ {
		nodeKey := nodeKeys[i]
		if self.IndexOf(nodeKey) < 0 {
			newNodeKeys[newNodeKeyIndex] = nodeKey
			newNodeKeyIndex++
		}
	}
	if newNodeKeyIndex == 0 {
		return false
	}
	newNodeKeys = newNodeKeys[:newNodeKeyIndex+1]
	self = append(self, newNodeKeys...)
	return true
}

func (self NodeKeySet) Remove(nodeKey uint32) bool {
	if len(self) == 0 {
		return false
	}
	index := self.IndexOf(nodeKey)
	if index < 0 {
		return false
	}
	copy(self[index:], self[index+1:])
	// self[length-1] = 0
	self = self[:len(self)-1]
	return true
}
