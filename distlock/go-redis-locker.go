package distlock

import (
	"fmt"
	goredis "github.com/go-redis/redis"
	"github.com/palantir/stacktrace"
	"strconv"
	"sync"
	"time"
	)

var goredisGlobal struct {
	luaUnlockSha    string
	luaKeepAliveSha string
	lock            sync.Mutex
}

type GoRedisClient interface {
	Get(key string) *goredis.StringCmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *goredis.Cmd
	ScriptExists(hashes ...string) *goredis.BoolSliceCmd
	ScriptLoad(script string) *goredis.StringCmd
	SetNX(key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
	TxPipeline() goredis.Pipeliner
	PSubscribe(channels ...string) *goredis.PubSub
}

type goredisLocker struct {
	redis     GoRedisClient
	localLock sync.Mutex
	ownerId   string
	lockKey   string
	ttl       time.Duration
	ttlSecStr     string
}

func NewGoRedisLocker(keyPrefix string, lockId string, ttlSeconds time.Duration, redis GoRedisClient) DistributedLocker {
	needsScriptLoad := false
	if goredisGlobal.luaUnlockSha == "" {
		needsScriptLoad = true
	} else if unlockScriptExists := redis.ScriptExists(goredisGlobal.luaUnlockSha).Val(); len(unlockScriptExists) == 0 || !unlockScriptExists[0] {
		needsScriptLoad = true
	}

	if needsScriptLoad {
		goredisGlobal.lock.Lock()
		goredisGlobal.luaUnlockSha = redis.ScriptLoad(luaUnlock).Val()
		goredisGlobal.lock.Unlock()
	}

	return &goredisLocker{
		redis:     redis,
		localLock: sync.Mutex{},
		ownerId:   generateRandomOwnerId(),
		lockKey:   keyPrefix + ":" + lockId,
		ttl:       ttlSeconds,
		ttlSecStr: strconv.FormatInt(int64(ttlSeconds.Seconds()), 10),
	}
}

func (r *goredisLocker) getLuaUnlockSha(forceReload bool) string {
	goredisGlobal.lock.Lock()
	defer goredisGlobal.lock.Unlock()

	if !forceReload && goredisGlobal.luaUnlockSha != "" {
		return goredisGlobal.luaUnlockSha
	}

	unlockScriptExists := r.redis.ScriptExists(goredisGlobal.luaUnlockSha).Val()[0]
	if !unlockScriptExists {
		goredisGlobal.luaUnlockSha = r.redis.ScriptLoad(luaUnlock).Val()
	}

	return goredisGlobal.luaUnlockSha
}

func (r *goredisLocker) getLuaKeepAliveSha(forceReload bool) string {
	goredisGlobal.lock.Lock()
	defer goredisGlobal.lock.Unlock()

	if !forceReload && goredisGlobal.luaUnlockSha != "" {
		return goredisGlobal.luaUnlockSha
	}

	unlockScriptExists := r.redis.ScriptExists(goredisGlobal.luaUnlockSha).Val()[0]
	if !unlockScriptExists {
		goredisGlobal.luaUnlockSha = r.redis.ScriptLoad(luaKeepAlive).Val()
	}

	return goredisGlobal.luaUnlockSha
}

func (r *goredisLocker) GetLockStatus() (result LockStatusResult) {
	r.localLock.Lock()
	defer r.localLock.Unlock()

	cmd := r.redis.Get(r.lockKey)
	result.Error = cmd.Err()
	lockOwnerId := cmd.Val()

	if result.Error == goredis.Nil {
		result.Error = nil
	}

	result.IsLocked = lockOwnerId != ""
	result.IsOwner = lockOwnerId == r.ownerId
	return
}

func (r *goredisLocker) WaitUntilUnlock(deadline time.Time) error {

	for {
		if time.Now().After(deadline) {
			return stacktrace.NewError("exceeded deadline for WaitUntilUnlock")
		}
		status := r.TryUnlock()
		if !status.IsLocked {
			return nil
		}
		time.Sleep(DefaultPollingPeriod)
	}
}

func (r *goredisLocker) TryLock() (result LockStatusResult) {
	r.localLock.Lock()
	defer r.localLock.Unlock()

	cmd := r.redis.SetNX(r.lockKey, r.ownerId, r.ttl)

	result.Error = cmd.Err()
	result.IsOwner = cmd.Val()
	result.IsLocked = true

	if result.Error == goredis.Nil {
		result.Error = nil
	}
	return
}

func (r *goredisLocker) TryForceLock() (result LockStatusResult) {
	r.localLock.Lock()

	txn := r.redis.TxPipeline()
	txn.Del(r.lockKey)
	txn.Set(r.lockKey, r.ownerId, r.ttl)
	_, err := txn.Exec()
	if err == nil {
		r.localLock.Unlock()
		return r.GetLockStatus()
	}
	result.Error = err
	r.localLock.Unlock()
	return
}

func (r *goredisLocker) evalLua(shaGetter func(forceReload bool) string, keys []string, args ...interface{}) *goredis.Cmd {
	r.localLock.Lock()
	defer r.localLock.Unlock()

	scriptSuccess := false
	shouldForceReload := false
	var scriptErr error
	for !scriptSuccess {
		sha := shaGetter(shouldForceReload)
		cmd := r.redis.EvalSha(sha, keys, args...)
		scriptErr = cmd.Err()
		scriptSuccess = scriptErr == nil
		if !scriptSuccess {
			shouldForceReload = true
			continue
		}
		return cmd
	}
	return nil
}

func (r *goredisLocker) TryUnlock() (result LockStatusResult) {
	cmd := r.evalLua(r.getLuaUnlockSha, []string{r.lockKey}, r.ownerId)
	if cmd == nil {
		result.Error = stacktrace.NewError("unexpected error")
	}
	returnCode, err := cmd.Int()

	result.IsLocked = returnCode == 2 || returnCode == 0
	result.IsOwner = returnCode == 1 || returnCode == 0
	result.Error = err
	return
}

func (r *goredisLocker) KeepAlive() (result LockStatusResult) {
	cmd := r.evalLua(r.getLuaKeepAliveSha, []string{r.lockKey}, r.ownerId, r.ttlSecStr)
	if cmd == nil {
		result.Error = stacktrace.NewError("unexpected error")
	}
	returnCode, err := cmd.Int()

	result.IsLocked = returnCode == 1 || returnCode == 2
	result.IsOwner = returnCode == 1 || returnCode == 0
	result.Error = err
	fmt.Printf("KeepAlive Return Code: %d\n", returnCode)
	return
}
