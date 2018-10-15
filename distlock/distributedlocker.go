package distlock

import "time"

type DistributedLocker interface {
	GetLockStatus() LockStatusResult
	WaitUntilUnlock(deadline time.Time) (err error)
	TryLock() LockStatusResult
	TryUnlock() LockStatusResult
	TryForceLock() LockStatusResult
	KeepAlive() (result LockStatusResult)
}

type LockStatusResult struct {
	IsLocked bool
	IsOwner  bool
	Error    error
}
type StatusEvaluator func(status LockStatusResult) (shouldContinue bool, err error)

var DefaultPollingPeriod time.Duration
var DefaultTtl time.Duration

func init() {
	var err error
	DefaultPollingPeriod, err = time.ParseDuration("1000ms")
	if err != nil {
		panic(err)
	}

	DefaultTtl, err = time.ParseDuration("60s")
	if err != nil {
		panic(err)
	}
}

func PollStatusUntil(lockToPoll DistributedLocker, period time.Duration, eval StatusEvaluator) error {
	shouldContinue := true
	var err error

	currentStatus := new(LockStatusResult)

	for shouldContinue {
		*currentStatus = lockToPoll.GetLockStatus()
		shouldContinue, err = eval(*currentStatus)
		if err != nil {
			return err
		}
	}

	return nil
}
