package com.latticeengines.cache.configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonSpringCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.domain.exposed.cache.CacheNames;

@Configuration
@ComponentScan
@EnableCaching
public class CacheBeansConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CacheBeansConfiguration.class);

    @Autowired
    private RedissonClient redisson;

    @Bean
    public CacheManager cacheManager(@Value("${cache.type}") String cacheType) {
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
        CacheConfig attributeRepoCacheConfig = new CacheConfig(0, maxIdleTime);
        attributeRepoCacheConfig.setMaxSize(100 * 2);

        CacheConfig dataLakeCMCacheConfig = new CacheConfig(0, maxIdleTime);
        dataLakeCMCacheConfig.setMaxSize(100 * 2);
        CacheConfig dataLakeStatsCacheConfig = new CacheConfig(0, maxIdleTime);
        dataLakeStatsCacheConfig.setMaxSize(100 * 2);

        CacheConfig entityCountCacheConfig = new CacheConfig(0, maxIdleTime);
        entityCountCacheConfig.setMaxSize(3000 * 2);
        CacheConfig entityDataCacheConfig = new CacheConfig(0, maxIdleTime);
        entityDataCacheConfig.setMaxSize(300 * 2);
        CacheConfig entityRatingCountCacheConfig = new CacheConfig(0, maxIdleTime);
        entityRatingCountCacheConfig.setMaxSize(3000 * 2);

        CacheConfig metadataCacheConfig = new CacheConfig(10 * 60 * 1000, 10 * 60 * 1000);

        CacheConfig sessionCacheConfig = new CacheConfig(5 * 60 * 1000, 15 * 60 * 1000);

        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheNames.AttributeRepoCache.name(), attributeRepoCacheConfig);
        config.put(CacheNames.DataLakeCMCache.name(), dataLakeCMCacheConfig);
        config.put(CacheNames.DataLakeStatsCache.name(), dataLakeStatsCacheConfig);

        config.put(CacheNames.EntityCountCache.name(), entityCountCacheConfig);
        config.put(CacheNames.EntityDataCache.name(), entityDataCacheConfig);
        config.put(CacheNames.EntityRatingCountCache.name(), entityRatingCountCacheConfig);
        config.put(CacheNames.MetadataCache.name(), metadataCacheConfig);
        config.put(CacheNames.SessionCache.name(), sessionCacheConfig);

        return new RedissonSpringCacheManager(redisson, config);
    }

    private CacheManager gaCacheManager() {
        CacheConfig sessionCacheConfig = new CacheConfig(5 * 60 * 1000, 5 * 60 * 1000);
        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheNames.SessionCache.name(), sessionCacheConfig);

        RedissonSpringCacheManager manager = new RedissonSpringCacheManager(redisson, config);
        manager.setCacheNames(Collections.singletonList(CacheNames.SessionCache.name()));
        return manager;
    }

    private CacheManager localCacheManager() {
        CompositeCacheManager compositeCacheManager = new CompositeCacheManager();
        compositeCacheManager.setCacheManagers(Arrays.asList(gaCacheManager()));
        return compositeCacheManager;
    }

}
