package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.apache.tomcat.jni.Local;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 1.缓存穿透解决
        // Shop shop = queryWithPassThrough(id);

        // 2.缓存击穿解决:互斥锁
        // Shop shop = queryWithMutex(id);

        // 3.缓存击穿解决:逻辑过期
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在!");
        }

        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){

        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1.从Redis中查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断存在情况
        if(StrUtil.isBlank(shopJson)) {
            // 3.不存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 4.命中，json反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1.未过期直接返回
            return shop;
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
                    saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        // 6.4.失败直接返回
        return shop;
    }

    /**
     * 缓存穿透解决
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1.从Redis中查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断存在情况
        if(StrUtil.isNotBlank(shopJson)) {
            // 3.存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // add: 命中的是否是空值
        if(shopJson != null) {
            return null;
        }

        // 4.不存在，则查询数据库
        Shop shop = getById(id);
        // 5.数据库中不存在，返回错误
        if(shop == null) {
            // add:空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回
        return shop;
    }

    /**
     * 缓存击穿解决：设置互斥锁
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1.从Redis中查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断存在情况
        if(StrUtil.isNotBlank(shopJson)) {
            // 3.存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // add: 命中的是否是空值
        if(shopJson != null) {
            return null;
        }

        // 4.实现缓存重建
        // 4.1.不存在，获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2.判断是否获取成功
            if(!isLock) {
                // 4.3.失败，休眠并充实
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4.成功，根据id查询数据库
            shop = getById(id);
                // 模拟重建延迟
            Thread.sleep(200);
            // 5.数据库中不存在，返回错误
            if(shop == null) {
                // add:空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.存在，写入Redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7.释放互斥锁
            unlock(lockKey);
        }

        // 8.返回
        return shop;
    }



    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL,TimeUnit.SECONDS);
        // 直接返回flag可能会导致拆箱，从而出现空指针
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null) {
            return Result.fail("店铺ID不能为空!");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
