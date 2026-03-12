package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        // 1.从Redis中查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断存在情况
        if(StrUtil.isNotBlank(json)) {
            // 3.存在直接返回
            return JSONUtil.toBean(json, type);
        }

        // add: 命中的是否是空值
        if(json != null) {
            return null;
        }

        // 4.不存在，则查询数据库
        R r = dbFallback.apply(id);
        // 5.数据库中不存在，返回错误
        if(r == null) {
            // add:空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入Redis
        this.set(key, r, time, timeUnit);
        // 7.返回
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, String lockKeyPrefix, ID id, Class<R> type, Long time, TimeUnit timeUnit,
            Function<ID, R> dbFallback) {

        String key = keyPrefix + id;
        // 1.从Redis中查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断存在情况
        if(StrUtil.isBlank(json)) {
            // 3.不存在直接返回
            return JSONUtil.toBean(json, type);
        }
        // 4.命中，json反序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1.未过期直接返回
            return r;
        }
        // 5.2.过期，尝试缓存重建
        // 6.缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        // 6.1.获取互斥锁
        boolean isLock = tryLock(lockKey);
        // 6.2.判断获取锁
        if(isLock){
            // 6.3.成功，开启独立线程实现重建
            // 用线程池
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, timeUnit);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        // 6.4.失败直接返回
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL,TimeUnit.SECONDS);
        // 直接返回flag可能会导致拆箱，从而出现空指针
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

}
