-- 锁的key
local key = KEYS[1]
--当前线程标识
local threadId = ARGV[1]

-- 获取锁中的线程标识 get key
local id = redis.call('get', key)
-- 比较标识是否一样
if(id == threadId) then
    -- 释放锁
    return redis.call('del', key)
end
return 0