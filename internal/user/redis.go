package user

import "github.com/redis/go-redis/v9"

func codeKey(phone string) string {
	return "login:code:{" + phone + "}"
}

func sessionKey(token string) string {
	return "login:token:" + token
}

// The cooldown survives successful login and applies across API instances.
var issueCode = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], 'code', ARGV[1], 'attempts', 0)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
return 1
`)

// A successful verification consumes the code in the same Redis operation.
var consumeCode = redis.NewScript(`
local code = redis.call('HGET', KEYS[1], 'code')
if not code then
    return 0
end
if code == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
if redis.call('HINCRBY', KEYS[1], 'attempts', 1) >= tonumber(ARGV[2]) then
    redis.call('DEL', KEYS[1])
end
return 0
`)

// Renew only managed sessions close to expiry, in the same operation as the read.
var readSession = redis.NewScript(`
local fields = redis.call('HMGET', KEYS[1], 'id', 'nickName', 'icon', 'renew')
if not fields[1] then
    return {}
end
if fields[4] == '1' then
    local ttl = redis.call('PTTL', KEYS[1])
    if ttl <= 0 then
        return {}
    end
    if ttl <= tonumber(ARGV[1]) then
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
    end
end
return {fields[1], fields[2] or '', fields[3] or ''}
`)
