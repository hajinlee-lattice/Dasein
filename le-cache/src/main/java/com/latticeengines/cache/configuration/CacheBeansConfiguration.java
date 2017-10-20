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
        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheNames.SessionCache.name(), new CacheConfig(5 * 60 * 1000, 5 * 60 * 1000));
        config.put(CacheNames.EntityCache.name(), new CacheConfig(0, 0));
        return new RedissonSpringCacheManager(redisson, config);
    }

}
