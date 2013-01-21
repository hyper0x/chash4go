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
	switch {
	case index >= length:
		matchedNode = self.nodes[0]
	case index == (length - 1):
		matchedNode = self.nodes[index]
		if matchedNode.Key == nodeKey {
			matchedNode = self.nodes[0]
		}
	default:
		matchedNode = self.nodes[index]
		if matchedNode.Key == nodeKey {
			matchedNode = self.nodes[index+1]
		}
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
	index := sort.Search(length, func(i int) bool { return self.nodes[i].Key >= nodeKey })
	if index < length {
		if self.nodes[index].Key != nodeKey {
			return false
		}
		copy(self.nodes[index:], self.nodes[index+1:])
		// self.nodes[length-1] = Node{}
		self.nodes = self.nodes[:length-1]
		return true
	}
	return false
}
