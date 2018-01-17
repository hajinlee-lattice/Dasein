package com.latticeengines.cache.configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.redisson.api.RedissonClient;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonSpringCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.cache.annotation.CachingConfigurer;
import com.latticeengines.domain.exposed.cache.CacheName;

@Configuration
@EnableCaching
public class CacheBeansConfiguration implements CachingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(CacheBeansConfiguration.class);

    @Autowired
    private RedissonClient redisson;

    @Value("${cache.type}")
    private String cacheType;

    @Bean
    @DependsOn("redisson")
    @Override
    public CacheManager cacheManager() {
        switch (cacheType) {
        case "redis":
            log.info("using redis cache manager");
            return redisCacheManager();
        case "local":
        default:
            log.info("using local cache manager");
            return localCacheManager();
        }
    }

    private CacheManager redisCacheManager() {
        long maxIdleTime = 5 * 24 * 60 * 60 * 1000;

        CacheConfig dataLakeCMCacheConfig = new CacheConfig(0, maxIdleTime);
        dataLakeCMCacheConfig.setMaxSize(800 * 2);
        CacheConfig dataLakeStatsCacheConfig = new CacheConfig(0, maxIdleTime);
        dataLakeStatsCacheConfig.setMaxSize(800 * 2);

        CacheConfig entityCountCacheConfig = new CacheConfig(0, maxIdleTime);
        entityCountCacheConfig.setMaxSize(3000 * 8 * 2);
        CacheConfig entityDataCacheConfig = new CacheConfig(0, maxIdleTime);
        entityDataCacheConfig.setMaxSize(300 * 8 * 2);
        CacheConfig entityRatingCountCacheConfig = new CacheConfig(0, maxIdleTime);
        entityRatingCountCacheConfig.setMaxSize(3000 * 8 * 2);

        CacheConfig metadataCacheConfig = new CacheConfig(10 * 60 * 1000, 10 * 60 * 1000);

        CacheConfig sessionCacheConfig = new CacheConfig(5 * 60 * 1000, 15 * 60 * 1000);

        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheName.DataLakeCMCache.name(), dataLakeCMCacheConfig);
        config.put(CacheName.DataLakeStatsCache.name(), dataLakeStatsCacheConfig);

        config.put(CacheName.EntityCountCache.name(), entityCountCacheConfig);
        config.put(CacheName.EntityDataCache.name(), entityDataCacheConfig);
        config.put(CacheName.EntityRatingCountCache.name(), entityRatingCountCacheConfig);
        config.put(CacheName.MetadataCache.name(), metadataCacheConfig);
        config.put(CacheName.SessionCache.name(), sessionCacheConfig);

        return new RedissonSpringCacheManager(redisson, config);
    }

    private CacheManager gaCacheManager() {
        CacheConfig sessionCacheConfig = new CacheConfig(5 * 60 * 1000, 5 * 60 * 1000);
        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheName.SessionCache.name(), sessionCacheConfig);

        RedissonSpringCacheManager manager = new RedissonSpringCacheManager(redisson, config);
        manager.setCacheNames(Collections.singletonList(CacheName.SessionCache.name()));
        return manager;
    }

    private CacheManager localCacheManager() {
        CompositeCacheManager compositeCacheManager = new CompositeCacheManager();
        compositeCacheManager.setCacheManagers(Arrays.asList(gaCacheManager()));
        return compositeCacheManager;
    }

    @Override
    public CacheResolver cacheResolver() {
        return null;
    }

    @Override
    public KeyGenerator keyGenerator() {
        return new SimpleKeyGenerator();
    }

    @Override
    public CacheErrorHandler errorHandler() {
        return new CacheErrorHandler() {

            @Override
            public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
                log.error(ExceptionUtils.getStackTrace(exception));
            }

            @Override
            public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
                throw exception;
            }

            @Override
            public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {
                throw exception;
            }

            @Override
            public void handleCacheClearError(RuntimeException exception, Cache cache) {
                throw exception;
            }
        };
    }
}
