package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
@Slf4j
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryByRedis() {
        String prefix = RedisConstants.CACHE_SHOPTYPE_KEY + "*";
        // 步骤1：模糊查询匹配的 Key（生产环境建议替换为 scan）
        Set<String> keys = stringRedisTemplate.keys(prefix);

        // 步骤2：判断 Key 是否存在（先判空，再判断是否为空集合）
        if (keys != null && !keys.isEmpty()) {
            List<ShopType> typeList = new ArrayList<>();
            // 优化：批量获取 Value，减少网络交互
            List<String> jsonList = stringRedisTemplate.opsForValue().multiGet(keys);
            if (jsonList != null) {
                for (String json : jsonList) {
                    ShopType shopType = JSONUtil.toBean(json, ShopType.class);
                    typeList.add(shopType);
                }
            }
            // 若缓存中有数据，直接返回（按 sort 排序，和数据库逻辑一致）
            if (!typeList.isEmpty()) {
                typeList.sort((a, b) -> a.getSort() - b.getSort());
                return Result.ok(typeList);
            }
        }

        // 步骤3：缓存未命中，查询数据库（MyBatis-Plus list() 不会返回 null）
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if (typeList.isEmpty()) {
            return Result.fail("数据不存在");
        }

        // 步骤4：写入 Redis 缓存（设置过期时间，避免数据不一致）
        for (ShopType type : typeList) {
            String key = RedisConstants.CACHE_SHOPTYPE_KEY + type.getId();
            // 设置过期时间（比如 30 分钟，可配置在 RedisConstants 中）
            stringRedisTemplate.opsForValue().set(
                    key,
                    JSONUtil.toJsonStr(type),
                    RedisConstants.CACHE_SHOPTYPE_TTL,
                    TimeUnit.MINUTES
            );
        }

        // 步骤5：返回结果
        return Result.ok(typeList);
    }
}