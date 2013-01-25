package chash4go

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestSimpleHashRing(t *testing.T) {
	servers := [...]string{"10.11.156.71:2181", "10.11.5.145:2181", "10.11.5.164:2181", "192.168.106.63:2181", "192.168.106.64:2181"}
	shr := SimpleHashRing{}
	// begin - test about init & build & add target
	if shr.Status() != UNINITIALIZED {
		t.Errorf("The status '%v' should '%v'. ", shr.Status(), UNINITIALIZED)
		t.FailNow()
	}
	err := shr.Build(500)
	if err != nil {
		t.Errorf("Build hash ring Error: %s", err)
		t.FailNow()
	}
	t.Logf("The hash ring is builded.")
	if shr.Status() != BUILDED {
		t.Errorf("The status '%v' should '%v'. ", shr.Status(), BUILDED)
		t.FailNow()
	}
	t.Logf("Add servers (%v)...", servers)
	for _, s := range servers {
		_, err := shr.AddTarget(s)
		if err != nil {
			t.Errorf("Adding server Error: %s", err)
			t.FailNow()
		}
	}
	// end - test about init & build & add target
	// begin - test about check
	nodeCheckFunc := func(server string) bool {
		if len(server) == 0 {
			return false
		}
		return true
	}
	err = shr.Check(nodeCheckFunc)
	if err != nil {
		t.Errorf("Check Error: %s", err)
		t.FailNow()
	}
	t.Logf("Check is OK.")
	done, err := shr.StartCheck(nodeCheckFunc, uint16(1))
	if err != nil {
		t.Errorf("Starting check Error: %s", err)
		t.FailNow()
	}
	if !done {
		t.Errorf("Starting check is FAILING.")
		t.FailNow()
	}
	t.Logf("The checker is started.")
	if !shr.InChecking() {
		t.Logf("The checker should be started.")
		t.FailNow()
	}
	time.Sleep(3 * time.Second)
	done, err = shr.StopCheck()
	if err != nil {
		t.Errorf("Stoping check Error: %s", err)
		t.FailNow()
	}
	if !done {
		t.Errorf("Stoping check is FAILING.")
		t.FailNow()
	}
	if shr.InChecking() {
		t.Logf("The checker should be stopped.")
		t.FailNow()
	}
	t.Logf("The checker is stopped.")
	// end - test about check
	// begin - test about get target & remove target
	key := "chash_test"
	target, err := shr.GetTarget(key)
	if err != nil {
		t.Errorf("Getting target Error: %s", err)
		t.FailNow()
	}
	expectedTarget := "192.168.106.64:2181"
	t.Logf("The target of '%s' (1st): %s", key, target)
	if target != expectedTarget {
		t.Errorf("The target '%s' of key '%s' should be '%s'.", target, key, expectedTarget)
		t.FailNow()
	}
	done, err = shr.RemoveTarget(target)
	if err != nil {
		t.Errorf("Removing target '%s' Error: %s", target, err)
		t.FailNow()
	}
	if !done {
		t.Errorf("Removing target '%s' is FAILING.", target)
		t.FailNow()
	}
	t.Logf("Removed target : %s", target)
	target, err = shr.GetTarget(key)
	if err != nil {
		t.Errorf("Getting target Error: %s", err)
		t.FailNow()
	}
	expectedTarget = "10.11.5.145:2181"
	t.Logf("The target of '%s' (2nd): %s", key, target)
	if target != expectedTarget {
		t.Errorf("The target '%s' of key '%s' should be '%s'.", target, key, expectedTarget)
		t.FailNow()
	}
	// end - test about get target & remove target
	// begin - test about destroy
	err = shr.Destroy()
	if err != nil {
		t.Errorf("Destroy hash ring Error: %s", err)
		t.FailNow()
	}
	t.Logf("The hash ring is destroyed.")
	if shr.Status() != DESTROYED {
		t.Errorf("The status '%v' should '%v'. ", shr.Status(), DESTROYED)
		t.FailNow()
	}
	// end - test about destroy
}

func TestSimpleHashRingForBenchmark(t *testing.T) {
	debugTag := false
	t.Logf("Starting benchmark of hash ring...")
	servers := [...]string{"10.11.156.71:2181", "10.11.5.145:2181", "10.11.5.164:2181", "192.168.106.63:2181", "192.168.106.64:2181"}
	shr := SimpleHashRing{}
	t.Logf("Build hash ring...")
	err := shr.Build(500)
	if err != nil {
		t.Errorf("Build hash ring Error: %s", err)
		t.FailNow()
	}
	t.Logf("Add servers (%v)...", servers)
	for _, s := range servers {
		_, err := shr.AddTarget(s)
		if err != nil {
			t.Errorf("Adding server Error: %s", err)
			t.FailNow()
		}
	}
	loopNumbers := []int{10000, 20000, 50000, 100000, 200000, 300000, 400000, 500000}
	for _, loopNumber := range loopNumbers {
		keys := make([]string, loopNumber)
		for i := 0; i < loopNumber; i++ {
			key := strconv.FormatInt(rand.Int63n(int64(9999999999999999)), 16)
			if len(key) > 10 {
				key = key[:10]
			}
			keys[i] = key
		}
		ns1 := time.Now().UnixNano()
		for i := 0; i < loopNumber; i++ {
			key := keys[i]
			target, err := shr.GetTarget(key)
			if err != nil {
				t.Errorf("Getting target Error (%v): %s", i, err)
				t.FailNow()
			}
			if debugTag {
				t.Logf("The target of key '%s' is '%s'. (%d)\n", key, target, i)
			}
			if len(target) == 0 {
				t.Errorf("The target of key '%s' is EMPTY!", key)
				t.FailNow()
			}
		}
		ns2 := time.Now().UnixNano()
		totalCostNs := ns2 - ns1
		totalCost := float64(totalCostNs) / float64(1000)
		eachCost := float64(totalCost) / float64(loopNumber)
		t.Logf("Benchmark Result (loopNumber=%d) - Total cost (microsecond): %f, Each cost (microsecond): %f.\n", loopNumber, totalCost, eachCost)
	}
	t.Logf("The benchmak of hash ring is end.")
}
