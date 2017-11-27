package com.latticeengines.cache.test.annotation;

import java.util.HashMap;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonSpringCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@EnableCaching
public class CacheBeansConfiguration {

    @Autowired
    private RedissonClient redisson;

    @Bean
    @DependsOn("redisson")
    public CacheManager redisTestCacheManager() {

        CacheConfig testCacheConfig = new CacheConfig(5 * 1000, 15 * 60 * 1000);

        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put("Test", testCacheConfig);

        return new RedissonSpringCacheManager(redisson, config);
    }
}
