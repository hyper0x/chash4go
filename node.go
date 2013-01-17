package chash4go

import (
	"sort"
)

type Node struct {
	Key    uint32
	Target string
}

func (self Node) Equals(other Node) bool {
	if self.Key != other.Key || self.Target != other.Target {
		return false
	}
	return true
}

/* 
 * A non-thread-safe ordered ring-like set
 */
type NodeRing struct {
	nodes []Node
}

func (self *NodeRing) Len() int {
	return len(self.nodes)
}

func (self *NodeRing) Less(i, j int) bool {
	return self.nodes[i].Key < self.nodes[j].Key
}

func (self *NodeRing) Swap(i, j int) {
	self.nodes[i], self.nodes[j] = self.nodes[j], self.nodes[i]
}

func (self *NodeRing) GetAll() []Node {
	result := make([]Node, len(self.nodes))
	copy(result, self.nodes)
	return result
}

func (self *NodeRing) Get(nodeKey uint32) *Node {
	length := len(self.nodes)
	if length == 0 {
		return nil
	}
	index := sort.Search(length, func(i int) bool { return self.nodes[i].Key == nodeKey })
	if index < length {
		matchedNode := self.nodes[index]
		return &Node{matchedNode.Key, matchedNode.Target}
	}
	return nil
}

func (self *NodeRing) Next(nodeKey uint32) *Node {
	length := len(self.nodes)
	if length == 0 {
		return nil
	}
	index := sort.Search(length, func(i int) bool { return self.nodes[i].Key >= nodeKey })
	var matchedNode Node
	if index >= length {
		matchedNode = self.nodes[0]
	} else {
		matchedNode = self.nodes[index]
	}
	return &Node{matchedNode.Key, matchedNode.Target}
}

func (self *NodeRing) Add(nodes ...Node) bool {
	paramLength := len(nodes)
	if paramLength == 0 {
		return false
	}
	selfLength := len(self.nodes)
	if selfLength == 0 {
		self.nodes = append(self.nodes, nodes...)
		return true
	}
	newNodes := make([]Node, paramLength)
	newNodeIndex := 0
	for i := 0; i < paramLength; i++ {
		node := nodes[i]
		index := sort.Search(selfLength, func(i int) bool { return self.nodes[i].Key == node.Key })
		if index >= selfLength {
			newNodes[newNodeIndex] = node
			newNodeIndex++
		} else {
			self.nodes[index].Target = node.Target
		}
	}
	if newNodeIndex == 0 {
		return false
	}
	newNodes = newNodes[:newNodeIndex]
	self.nodes = append(self.nodes, newNodes...)
	sort.Sort(self)
	return true
}

func (self *NodeRing) Remove(nodeKey uint32) bool {
	length := len(self.nodes)
	if length == 0 {
		return false
	}
	index := sort.Search(length, func(i int) bool { return self.nodes[i].Key == nodeKey })
	if index < length {
		copy(self.nodes[index:], self.nodes[index+1:])
		// self.nodes[length-1] = Node{}
		self.nodes = self.nodes[:length-1]
		return true
	}
	return false
}

/* 
 * A non-thread-safe inserting-order set
 */
type NodeKeySet struct {
	keys []uint32
}

func (self *NodeKeySet) Len() int {
	return len(self.keys)
}

func (self *NodeKeySet) IndexOf(nodeKey uint32) int {
	index := sort.Search(len(self.keys), func(i int) bool { return self.keys[i] == nodeKey })
	if index >= len(self.keys) {
		return -1
	}
	return index
}

func (self *NodeKeySet) GetAll() []uint32 {
	result := make([]uint32, len(self.keys))
	copy(result, self.keys)
	return result
}

func (self *NodeKeySet) Add(nodeKeys ...uint32) bool {
	paramLength := len(nodeKeys)
	if paramLength == 0 {
		return false
	}
	selfLength := len(self.keys)
	if selfLength == 0 {
		self.keys = append(self.keys, nodeKeys...)
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
	newNodeKeys = newNodeKeys[:newNodeKeyIndex]
	self.keys = append(self.keys, newNodeKeys...)
	return true
}

func (self *NodeKeySet) Remove(nodeKey uint32) bool {
	if len(self.keys) == 0 {
		return false
	}
	index := self.IndexOf(nodeKey)
	if index < 0 {
		return false
	}
	copy(self.keys[index:], self.keys[index+1:])
	// self.keys[length-1] = 0
	self.keys = self.keys[:len(self.keys)-1]
	return true
}
