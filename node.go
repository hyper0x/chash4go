package chash4go

import (
	"fmt"
	"sort"
)

type Node struct {
	Key    uint64
	Target string
}

func (self Node) Equals(other Node) bool {
	if self.Key != other.Key || self.Target != other.Target {
		return false
	}
	return true
}

type NodeRingIterator func() (*Node, bool)

/* 
 * A non-thread-safe ordered ring-like set
 */
type NodeRing struct {
	nodeKeys []uint64
	nodeMap  map[uint64]string
}

func (self *NodeRing) Len() int {
	return len(self.nodeKeys)
}

func (self *NodeRing) Less(i, j int) bool {
	return self.nodeKeys[i] < self.nodeKeys[j]
}

func (self *NodeRing) Swap(i, j int) {
	self.nodeKeys[i], self.nodeKeys[j] = self.nodeKeys[j], self.nodeKeys[i]
}

func (self *NodeRing) GetIterator() NodeRingIterator {
	return func(nodeKeys []uint64, nodeMap map[uint64]string) NodeRingIterator {
		index := 0
		return func() (*Node, bool) {
			if index >= 0 && index < len(nodeKeys) {
				key := nodeKeys[index]
				index++
				return &Node{Key: key, Target: nodeMap[key]}, true
			}
			return nil, false
		}
	}(self.nodeKeys, self.nodeMap)
}

func (self *NodeRing) GetAllNodeKey() []uint64 {
	length := len(self.nodeKeys)
	result := make([]uint64, length)
	copy(result[:length], self.nodeKeys[:length])
	return result
}

func (self *NodeRing) GetByIndex(index int) *Node {
	if index < 0 || index >= len(self.nodeKeys) {
		return nil
	}
	nodeKey := self.nodeKeys[index]
	target, exists := self.nodeMap[nodeKey]
	if !exists {
		return nil
	}
	return &Node{Key: nodeKey, Target: target}
}

func (self *NodeRing) Get(nodeKey uint64) *Node {
	length := len(self.nodeKeys)
	if length == 0 {
		return nil
	}
	target, exists := self.nodeMap[nodeKey]
	if exists {
		return &Node{nodeKey, target}
	}
	return nil
}

func (self *NodeRing) Next(nodeKey uint64) *Node {
	length := len(self.nodeKeys)
	if length == 0 {
		return nil
	}
	index := sort.Search(length, func(i int) bool { return self.nodeKeys[i] >= nodeKey })
	var matchedKey uint64
	if index >= length {
		matchedKey = self.nodeKeys[0]
	} else {
		matchedKey = self.nodeKeys[index]
	}
	return &Node{matchedKey, self.nodeMap[matchedKey]}
}

func (self *NodeRing) Add(nodes ...Node) ([]uint64, bool) {
	paramLength := len(nodes)
	if paramLength == 0 {
		return nil, false
	}
	newNodeKeys := make([]uint64, paramLength)
	newIndex := 0
	for i := 0; i < paramLength; i++ {
		node := nodes[i]
		nodeKey := node.Key
		_, exists := self.nodeMap[nodeKey]
		if !exists {
			self.nodeMap[nodeKey] = node.Target
			lengthOfNew := len(newNodeKeys)
			indexOfNew := sort.Search(lengthOfNew, func(i int) bool { return newNodeKeys[i] >= nodeKey })
			if indexOfNew >= lengthOfNew || newNodeKeys[indexOfNew] != nodeKey {
				newNodeKeys[newIndex] = nodeKey
				newIndex++
			}
		}
	}
	if newIndex == 0 {
		return nil, false
	}
	newNodeKeys = newNodeKeys[:newIndex]
	self.nodeKeys = append(self.nodeKeys, newNodeKeys...)
	sort.Sort(self)
	kLen := len(self.nodeKeys)
	mLen := len(self.nodeMap)
	if kLen != mLen {
		panic(fmt.Sprintf("The length of keys & map is different! (lenOfKeySet=%v, lenOfMap=%v)", kLen, mLen))
	}
	return newNodeKeys, true
}

func (self *NodeRing) Remove(nodeKey uint64) bool {
	length := len(self.nodeKeys)
	if length == 0 {
		return false
	}
	index := sort.Search(length, func(i int) bool { return self.nodeKeys[i] >= nodeKey })
	if index < length {
		if self.nodeKeys[index] != nodeKey {
			return false
		}
		copy(self.nodeKeys[index:], self.nodeKeys[index+1:])
		// self.nodeKeys[length-1] = 0
		self.nodeKeys = self.nodeKeys[:length-1]
		delete(self.nodeMap, nodeKey)
		return true
	}
	return false
}

func NewNodeRing() *NodeRing {
	return &NodeRing{nodeKeys: make([]uint64, 0), nodeMap: make(map[uint64]string)}
}
