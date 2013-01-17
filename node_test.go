package chash4go

import (
	"testing"
)

func TestNodeRing(t *testing.T) {
	nr := NodeRing{}
	n1 := Node{1, "A"}
	if !nr.Add(n1) {
		t.Errorf("n1 add to nr: Failing.")
		t.FailNow()
	}
	t.Log("n1 is added to nr.")
	if nr.Len() != 1 {
		t.Errorf("nr length is not 1.")
		t.FailNow()
	}
	t.Log("nr length: 1.")
	n1r := nr.Get(1)
	if n1r.Key != n1.Key || n1r.Target != n1.Target {
		t.Errorf("n1r '%v' is not equals n1 '%v'.", *n1r, n1)
		t.FailNow()
	}
	n1.Target = "AA"
	n1r = nr.Get(1)
	if n1r.Equals(n1) {
		t.Errorf("n1r '%v' is not equals n1 '%v'.", *n1r, n1)
		t.FailNow()
	}
	t.Logf("n1r equals n1.")
	if nr.Add(n1) {
		t.Errorf("Is n1 '%v' NEW ?", n1)
		t.FailNow()
	}
	t.Logf("n1 has been added.")
	n2 := Node{2, "B"}
	n3 := Node{3, "C"}
	n4 := Node{4, "D"}
	n5 := Node{5, "E"}
	if !nr.Add(n2, n3, n4, n5) {
		t.Errorf("n2, n3, n4, n5 add to nr: Failing.")
		t.FailNow()
	}
	t.Log("n2, n3, n4, n5 is added to nr.")
	if nr.Len() != 5 {
		t.Errorf("nr length is not 5.")
		t.FailNow()
	}
}
