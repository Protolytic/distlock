package distlock

/**
	Return codes:

		-1 = was not locked to begin with
		0 = did not succeed in unlocking; still locked
		1 = successfully unlocked
		2 = locked by another owner; still locked
 */
const luaUnlock = `
local lockOwner = redis.call("get",KEYS[1])
if lockOwner ~= false then
	if lockOwner == ARGV[1] then
    	return redis.call("del",KEYS[1])
	end
	return 2
else
    return -1
end
`

/**
	Return codes:

		-1 = was not locked to begin with
		0 = did not succeed in keep-alive; did not keep-alive
		1 = successfully did keep-alive
		2 = locked by another owner; did not keep-alive
 */
const luaKeepAlive = `
local lockOwner = redis.call("get",KEYS[1])
if lockOwner ~= false then
	if lockOwner == ARGV[1] then
    	return redis.call("expire",KEYS[1],ARGV[2])
	end
	return 2
else
    return -1
end
`