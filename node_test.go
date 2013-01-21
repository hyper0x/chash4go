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
	nr := NodeRing{}
	n1 := Node{1, "A"}
	if !nr.Add(n1) {
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
	if nr.Add(n1) {
		t.Errorf("Is n1 '%v' NEW? (Add n1 again)", n1)
		t.FailNow()
	}
	t.Logf("n1 has been added. (Add n1 again)")
	n2 := Node{2, "B"}
	n3 := Node{3, "C"}
	n4 := Node{4, "D"}
	n5 := Node{5, "E"}
	if !nr.Add(n2, n3, n4, n5) {
		t.Errorf("n2, n3, n4, n5 add to nr: Failing.. (Add more)")
		t.FailNow()
	}
	t.Log("n2, n3, n4, n5 is added to nr.")
	if nr.Len() != 5 {
		t.Errorf("nr length is not 5. (Add more)")
		t.FailNow()
	}
	n2r := nr.Next(1)
	if !n2r.Equals(n2) {
		t.Errorf("n2r '%v' is not equals n2 '%v'. (Next of n1)", *n2r, n2)
		t.FailNow()
	}
	t.Logf("n2r equals n2. (Next)")
	nxr := nr.Next(5)
	if !nxr.Equals(n1) {
		t.Errorf("nxr '%v' is not equals n1 '%v'. (Next of n5)\n", *nxr, n1)
		t.FailNow()
	}
	nxr = nr.Next(6)
	if !nxr.Equals(n1) {
		t.Errorf("nxr '%v' is not equals n1 '%v'. (Next of nonexistent nodekey)", *nxr, n1)
		t.FailNow()
	}
	nodes := nr.GetAll()
	if len(nodes) != 5 {
		t.Errorf("The length '%v' of nodes is not '%v'. (GetAll)", len(nodes), 5)
		t.FailNow()
	}
	for i, n := range nodes {
		if n.Key != uint32(i+1) {
			t.Errorf("The key '%v' of node is not '%v'. (GetAll)", n.Key, i+1)
			t.FailNow()
		}
	}
	result := nr.Remove(3)
	if !result {
		t.Errorf("Remve node (key=%v) is FAILING. (Remove)", 3)
		t.FailNow()
	}
	t.Logf("The node (key=%v) is removed. (Remove)", 3)
	n3r := nr.Get(3)
	if n3r != nil {
		t.Errorf("Is n%v EXISTS?. (Get after Remove)", 3)
		t.FailNow()
	}
	nodes = nr.GetAll()
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		result := nr.Remove(node.Key)
		if !result {
			t.Errorf("Remove node (key=%v, index=%v) is FAILING. (Remove)", node.Key, i)
			t.FailNow()
		}
		t.Logf("The node (key=%v) is removed. (Remove)", node.Key)
	}
}
