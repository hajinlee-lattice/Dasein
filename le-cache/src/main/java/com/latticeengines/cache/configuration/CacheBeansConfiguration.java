package com.latticeengines.cache.configuration;

import java.util.HashMap;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonSpringCacheManager;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.domain.exposed.cache.CacheNames;

@Configuration
@ComponentScan
@EnableCaching
public class CacheBeansConfiguration {

    @Bean("redisCache")
    public CacheManager cacheManager(RedissonClient redisson) {
        CacheConfig entityCacheConfig = new CacheConfig(0, 0);
        entityCacheConfig.setMaxSize(3000 * 3 * 2);

        CacheConfig dataLakeCacheConfig = new CacheConfig(0, 0);
        dataLakeCacheConfig.setMaxSize(100 * 2 * 2);

        CacheConfig metadataCacheConfig = new CacheConfig(10 * 60 * 1000, 10 * 60 * 1000);

        CacheConfig sessionCacheConfig = new CacheConfig(5 * 60 * 1000, 5 * 60 * 1000);

        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheNames.DataLakeCache.name(), dataLakeCacheConfig);
        config.put(CacheNames.EntityCache.name(), entityCacheConfig);
        config.put(CacheNames.MetadataCache.name(), metadataCacheConfig);
        config.put(CacheNames.SessionCache.name(), sessionCacheConfig);

        return new RedissonSpringCacheManager(redisson, config);
    }

}
