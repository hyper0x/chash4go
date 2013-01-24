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

func BenchmarkSimpleHashRing(b *testing.B) {
	b.StopTimer()
	debugTag := false
	b.Logf("Starting benchmak of hash ring...")
	servers := [...]string{"10.11.156.71:2181", "10.11.5.145:2181", "10.11.5.164:2181", "192.168.106.63:2181", "192.168.106.64:2181"}
	shr := SimpleHashRing{}
	b.Logf("Build hash ring...")
	err := shr.Build(500)
	if err != nil {
		b.Errorf("Build hash ring Error: %s", err)
		b.FailNow()
	}
	b.Logf("Add servers (%v)...", servers)
	for _, s := range servers {
		_, err := shr.AddTarget(s)
		if err != nil {
			b.Errorf("Adding server Error: %s", err)
			b.FailNow()
		}
	}
	b.ResetTimer()
	b.StartTimer()
	for i := 1; i <= b.N; i++ {
		randNumber := rand.Int63n(int64(999999999999999))
		key := strconv.FormatInt(randNumber, 16)
		target, err := shr.GetTarget(key)
		if err != nil {
			b.Errorf("Getting target Error (%v): %s", i, err)
			b.FailNow()
		}
		if debugTag {
			b.Logf("The target of key '%s' is '%s'. (%d)\n", key, target, i)
		}
	}
	b.StopTimer()
	b.Logf("The benchmak of hash ring is end.")
}
