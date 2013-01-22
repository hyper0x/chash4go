package chash4go

import (
	"testing"
)

func TestNode(t *testing.T) {
	n1 := Node{1, "A"}
	n2 := Node{2, "B"}
	if n1.Equals(n2) {
		t.Errorf("n1 '%v' should not equals n2 '%v'. ", n1, n2)
		t.FailNow()
	}
	n2.Key = 1
	n2.Target = "A"
	if !n1.Equals(n2) {
		t.Errorf("n1 '%v' should equals n2 '%v'. ", n1, n2)
		t.FailNow()
	}
}

func TestNodeRing(t *testing.T) {
	nr := NewNodeRing()
	n1 := Node{1, "A"}
	_, done := nr.Add(n1)
	if !done {
		t.Errorf("n1 add to nr: Failing. (Add n1)")
		t.FailNow()
	}
	t.Log("n1 is added to nr. (Add n1)")
	if nr.Len() != 1 {
		t.Errorf("nr length is not 1. (Len)")
		t.FailNow()
	}
	t.Log("nr length: 1.")
	n1r := nr.Get(1)
	if !n1r.Equals(n1) {
		t.Errorf("n1r '%v' is not equals n1 '%v'. (Add n1)", *n1r, n1)
		t.FailNow()
	}
	t.Logf("n1r equals n1.")
	n1.Target = "AA"
	n1r = nr.Get(1)
	if n1r.Equals(n1) {
		t.Errorf("n1r '%v' should not equals n1 '%v'. (Modify outer n1)", *n1r, n1)
		t.FailNow()
	}
	t.Logf("All: %v", nr.GetAllNodeKey())
	_, done = nr.Add(n1)
	if done {
		t.Errorf("Is n1 '%v' NEW? (Add n1 again)", n1)
		t.FailNow()
	}
	t.Logf("n1 has been added. (Add n1 again)")
	n2 := Node{2, "B"}
	n4 := Node{4, "D"}
	n6 := Node{6, "F"}
	t.Logf("NodeRing: %v", *nr)
	_, done = nr.Add(n2, n4, n6)
	if !done {
		t.Errorf("n2, n4, n6 add to nr: Failing.. (Add more)")
		t.Logf("NodeRing: %v", *nr)
		t.FailNow()
	}
	t.Logf("NodeRing: %v", *nr)
	t.Log("n2, n4, n6 is added to nr.")
	if nr.Len() != 4 {
		t.Errorf("nr length is not 4. (Add more)")
		t.FailNow()
	}
	n2r := nr.Next(2)
	if !n2r.Equals(n2) {
		t.Errorf("n2r '%v' is not equals n2 '%v'. (Next of 1)", *n2r, n2)
		t.FailNow()
	}
	t.Logf("n2r equals n2. (Next)")
	n5r := nr.Next(5)
	if !n5r.Equals(n6) {
		t.Errorf("n5r '%v' is not equals n4 '%v'. (Next of 3)", *n5r, n6)
		t.FailNow()
	}
	t.Logf("n2r equals n2. (Next)")
	nxr := nr.Next(5)
	if !nxr.Equals(n6) {
		t.Errorf("nxr '%v' is not equals n6 '%v'. (Next of 5)\n", *nxr, n6)
		t.FailNow()
	}
	nxr = nr.Next(7)
	n1.Target = "A"
	if !nxr.Equals(n1) {
		t.Errorf("nxr '%v' is not equals n1 '%v'. (Next of nonexistent nodekey)", *nxr, n1)
		t.FailNow()
	}
	if nr.Len() != 4 {
		t.Errorf("The length '%v' of nodes is not '%v'. (GetAll)", nr.Len(), 4)
		t.FailNow()
	}
	nrIter := nr.GetIterator()
	node, ok := nrIter()
	count := 0
	for ok {
		expectedKey := count * 2
		if count == 0 {
			expectedKey = count + 1
		}
		if node.Key != uint64(expectedKey) {
			t.Errorf("The key '%v' of node is not '%v'. (GetIterator)", node, expectedKey)
			t.FailNow()
		}
		count++
		node, ok = nrIter()
	}
	result := nr.Remove(4)
	if !result {
		t.Errorf("Remove node (key=%v) is FAILING. (Remove)", 4)
		t.FailNow()
	}
	t.Logf("The node (key=%v) is removed. (Remove)", 4)
	n3r := nr.Get(4)
	if n3r != nil {
		t.Errorf("Is n%v EXISTS?. (Get after Remove)", 4)
		t.FailNow()
	}
	for i := nr.Len() - 1; i >= 0; i-- {
		node := nr.GetByIndex(i)
		result := nr.Remove(node.Key)
		if !result {
			t.Errorf("Remove node (key=%v, index=%v) is FAILING. (Remove)", node.Key, i)
			t.FailNow()
		}
		t.Logf("The node (key=%v) is removed. (Remove)", node.Key)
	}
}
