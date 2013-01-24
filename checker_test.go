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
	t.Logf("Start the checker...")
	result := checker.Start(func() { countChan <- 1 })
	if !result {
		t.Errorf("The result is starting checker is FALSE! ")
		t.FailNow()
	}
	if !checker.InChecking() {
		t.Errorf("The Checker is not successful running! ")
		t.FailNow()
	}
	t.Logf("The checker is started.")
	timeoutSeconds := intervalSeconds * 3
	t.Logf("The timeoutSeconds is %v", timeoutSeconds)
	timeoutChan := time.After(time.Duration(timeoutSeconds) * time.Second)
	continueSign := make(chan bool)
	go func() {
		for {
			select {
			case i := <-countChan:
				count += i
				t.Logf("The count is %v.", count)
			case <-timeoutChan:
				t.Logf("Stop the checker...")
				result := checker.Stop()
				if !result {
					t.Errorf("The result is stopping checker is FALSE! ")
					t.FailNow()
				}
				continueSign <- true
				break
			}
		}
	}()
	<-continueSign
	t.Logf("The checker is stopped.")
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
