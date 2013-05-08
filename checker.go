package chash4go

import (
	"time"
)

type CheckFunc func()

type Checker interface {
	Start(checkFunc CheckFunc) bool
	Stop() bool
	InChecking() bool
}

type CycleChecker struct {
	IntervalSeconds uint16
	checkingTag     bool
	stopSign        chan bool
	count           uint64
}

func (self *CycleChecker) Start(checkFunc CheckFunc) bool {
	if self.checkingTag {
		logger.Warnln("Please stop before restart.")
		return false
	}
	if self.IntervalSeconds <= 0 {
		self.IntervalSeconds = 2
	}
	self.stopSign = make(chan bool, 1)
	self.count = 0
	tick := time.Tick(time.Duration(self.IntervalSeconds) * time.Second)
	go func() {
		for {
			select {
			case <-tick:
				checkFunc()
				self.count++
			case <-self.stopSign:
				logger.Infof("The checker will be stop. (count=%d)", self.count)
				break
			}
		}
	}()
	self.checkingTag = true
	return true
}

func (self *CycleChecker) Stop() bool {
	if !self.checkingTag {
		logger.Warnln("The checker were not started.")
		return false
	}
	self.checkingTag = false
	self.stopSign <- true
	return true
}

func (self *CycleChecker) InChecking() bool {
	return self.checkingTag
}

func NewChecker(intervalSeconds uint16) Checker {
	return interface{}(&CycleChecker{IntervalSeconds: intervalSeconds}).(Checker)
}
