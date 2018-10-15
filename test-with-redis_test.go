package main

import (
	"github.com/go-redis/redis"
	"github.com/mprdev/distlock/distlock"
	"sync"
	"testing"
	"time"
)

func TestWithRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	lockA1 := distlock.NewGoRedisLocker("distlocktest", "testA", time.Second * 5, client)
	lockA2 := distlock.NewGoRedisLocker("distlocktest", "testA", time.Second * 5, client)
	t.Logf("lockA1.TryLock() == %+v\n", lockA1.TryLock())

	wg := sync.WaitGroup{}
	wg.Add(4)

	go func(){
		err := lockA2.WaitUntilUnlock(time.Now().Add(time.Second * 10))
		if err != nil {
			t.Logf("Error waiting for lockA1 to unlock: %s\n", err.Error())
			wg.Done()
			return
		}
		time.Sleep(time.Second * 1) // just so Printf makes sense
		res := lockA2.GetLockStatus()
		t.Logf("Successfully waited for lockA1 to unlock. lockA2.GetLockStatus() == %+v\n", res)
		wg.Done()
	}()

	go func() {
		time.Sleep(time.Second * 2)
		t.Logf("lockA1.TryUnlock() == %+v\n", lockA1.TryUnlock())
		wg.Done()
	}()


	lockB1 := distlock.NewGoRedisLocker("distlocktest", "testB", time.Second * 5, client)
	lockB2 := distlock.NewGoRedisLocker("distlocktest", "testB", time.Second * 5, client)
	t.Logf("lockB1.TryLock() == %+v\n", lockB1.TryLock())

	go func() {
		time.Sleep(time.Second * 3)
		t.Logf("lockB2.TryForceLock() == %+v\n", lockB2.TryForceLock())
		wg.Done()
	}()

	go func() {
		time.Sleep(time.Second * 4)
		t.Logf("lockB1.TryUnlock() == %+v\n", lockB1.TryUnlock())
		wg.Done()
	}()

	wg.Wait()

	t.Logf("lockA1.GetLockStatus() == %+v\n", lockA1.GetLockStatus())
	t.Logf("lockB1.GetLockStatus() == %+v\n", lockB1.GetLockStatus())
	t.Logf("lockB2.GetLockStatus() == %+v\n", lockB2.GetLockStatus())

	t.Logf("\n\nNow testing KeepAlive...\n\n")

	lockC1 := distlock.NewGoRedisLocker("distlocktest", "testC", time.Second * 5, client)

	keepAliveStart := time.Now()
	lockC1.TryLock()

	wg.Add(2)

	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			t.Logf("lockC1.KeepAlive() == %+v\n", lockC1.KeepAlive())
		}
		wg.Done()
	}()

	lockC2 := distlock.NewGoRedisLocker("distlocktest", "testC", time.Second * 5, client)
	go func() {
		status := lockC2.WaitUntilUnlock(time.Now().Add(time.Second * 15))
		delta := time.Now().Sub(keepAliveStart).Seconds()
		t.Logf("Successfully waited for lock after %.2f seconds\n\tstatus: %+v\n", delta, status)
		wg.Done()
	}()

	wg.Wait()
}