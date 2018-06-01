package com.latticeengines.cache.test.annotation;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.latticeengines.domain.exposed.cache.CacheName;

@Configuration
@EnableCaching
public class CacheBeansConfiguration {

    @Inject
    private RedisConnectionFactory lettuceConnectionFactory;

    @Bean
    public CacheManager redisTestCacheManager() {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofSeconds(1)) //
                .prefixKeysWith("a").disableCachingNullValues();

        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();
        cacheConfigs.put(CacheName.SessionCache.name(),
                RedisCacheConfiguration.defaultCacheConfig()//
                        .entryTtl(Duration.ofMinutes(20)) //
                        .prefixKeysWith(CacheName.SessionCache.name()) //
                        .disableCachingNullValues());
        return RedisCacheManager
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory))//
                .cacheDefaults(config) //
                .withInitialCacheConfigurations(cacheConfigs) //
                .transactionAware()//
                .build();
    }
}
