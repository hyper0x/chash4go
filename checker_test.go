package chash4go

import (
	"testing"
	"time"
)

func TestCycleChecker(t *testing.T) {
	intervalSeconds := 1
	t.Logf("The intervalSeconds is %v", intervalSeconds)
	checker := CycleChecker{IntervalSeconds: uint16(intervalSeconds)}
	count := 0
	t.Logf("The count is %v", count)
	countChan := make(chan int, 1)
	checker.Start(func() { countChan <- 1 })
	timeoutSeconds := intervalSeconds * 3
	t.Logf("The timeoutSeconds is %v", timeoutSeconds)
	timeoutChan := time.After(time.Duration(timeoutSeconds) * time.Second)
	continueSign := make(chan bool)
	go func() {
		for {
			select {
			case i := <-countChan:
				t.Logf("count += %v.", i)
				count += i
			case <-timeoutChan:
				t.Logf("Stop the cheker...")
				checker.Stop()
				continueSign <- true
			}
		}
	}()
	<-continueSign
	t.Logf("The checker is stoped.")
	if checker.InChecking() {
		t.Errorf("The Checker is still running! ")
		t.FailNow()
	}
	if count != timeoutSeconds {
		t.Errorf("The count '%v' should equals timeoutSeconds '%v'. ", count, timeoutSeconds)
		t.FailNow()
	}
	t.Logf("The count is %v. It's OK.", count)
}
