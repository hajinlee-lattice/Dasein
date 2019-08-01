package com.latticeengines.cache.configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.cache.CacheName;

@Configuration
@EnableCaching
public class LettuceCacheBeansConfiguration implements CachingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(LettuceCacheBeansConfiguration.class);

    @Inject
    private RedisConnectionFactory lettuceConnectionFactory;

    @Value("${cache.type}")
    private String cacheType;

    @Value("${cache.redis.command.timeout.min}")
    private int redisTimeout;

    @Bean
    @Lazy
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

    public static RedisSerializer<?> getValueSerializer() {
        Jackson2JsonRedisSerializer<?> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        return jackson2JsonRedisSerializer;
    }

    public CacheManager redisCacheManager() {
        long ttl = 10;
        RedisCacheConfiguration sessionCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofMinutes(20)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.SessionCacheName));

        RedisCacheConfiguration emrClusterCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofMinutes(10)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.EMRClusterCacheName));

        // =========================
        // BEGIN: datalake service
        // =========================
        RedisCacheConfiguration dataLakeCMCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.DataLakeCMCacheName));
        RedisCacheConfiguration dataLakeTopNTreeCache = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.DataLakeTopNTreeCache));
        RedisCacheConfiguration dataLakeStatsCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.DataLakeStatsCubesCache));
        // =========================
        // END: datalake service
        // =========================

        // =========================
        // BEGIN: objectapi proxy
        // =========================
        RedisCacheConfiguration objectApiCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(1)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.ObjectApiCacheName));
        // =====================
        // END: objectapi proxy
        // =====================

        // =========================
        // BEGIN: dante microservice
        // =========================
        RedisCacheConfiguration dantePreviewTokenCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.DantePreviewTokenCacheName));
        // =======================
        // END: dante microservice
        // =======================

        RedisCacheConfiguration servingMetadataCache = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.ServingMetadataCacheName));

        RedisCacheConfiguration tableRoleMetadataCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.TableRoleMetadataCacheName));

        RedisCacheConfiguration dataCloudVersionCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofHours(1)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.DataCloudVersionCacheName));

        RedisCacheConfiguration activeStackInfoCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofMinutes(10)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.ActiveStackInfoCacheName));

        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();
        cacheConfigs.put(CacheName.Constants.SessionCacheName, sessionCacheConfig);
        cacheConfigs.put(CacheName.Constants.DataLakeCMCacheName, dataLakeCMCacheConfig);
        cacheConfigs.put(CacheName.Constants.DataLakeTopNTreeCache, dataLakeTopNTreeCache);
        cacheConfigs.put(CacheName.Constants.DataLakeStatsCubesCache, dataLakeStatsCacheConfig);

        cacheConfigs.put(CacheName.Constants.ObjectApiCacheName, objectApiCacheConfig);
        cacheConfigs.put(CacheName.Constants.ServingMetadataCacheName, servingMetadataCache);
        cacheConfigs.put(CacheName.Constants.TableRoleMetadataCacheName, tableRoleMetadataCacheConfig);
        cacheConfigs.put(CacheName.Constants.DantePreviewTokenCacheName, dantePreviewTokenCacheConfig);

        cacheConfigs.put(CacheName.Constants.DataCloudVersionCacheName, dataCloudVersionCacheConfig);
        cacheConfigs.put(CacheName.Constants.ActiveStackInfoCacheName, activeStackInfoCacheConfig);

        cacheConfigs.put(CacheName.Constants.EMRClusterCacheName, emrClusterCacheConfig);

        RedisCacheManager cacheManager = RedisCacheManager
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory))//
                .cacheDefaults(RedisCacheConfiguration.defaultCacheConfig()) //
                .withInitialCacheConfigurations(cacheConfigs) //
                .transactionAware()//
                .build();
        cacheManager.afterPropertiesSet();
        return cacheManager;
    }

    private String getPrefix(String s) {
        return s + "_";
    }

    private CacheManager gaCacheManager() {
        RedisCacheConfiguration sessionCacheConfig = RedisCacheConfiguration.defaultCacheConfig() //
                .entryTtl(Duration.ofMinutes(20)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.SessionCacheName));
        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();
        cacheConfigs.put(CacheName.Constants.SessionCacheName, sessionCacheConfig);
        RedisCacheManager cacheManager = RedisCacheManager
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory))//
                .cacheDefaults(RedisCacheConfiguration.defaultCacheConfig()) //
                .withInitialCacheConfigurations(cacheConfigs) //
                .transactionAware()//
                .build();
        cacheManager.afterPropertiesSet();
        return cacheManager;
    }

    private CacheManager localCacheManager() {
        CompositeCacheManager compositeCacheManager = new CompositeCacheManager(gaCacheManager());
        compositeCacheManager.setFallbackToNoOpCache(true);
        compositeCacheManager.afterPropertiesSet();
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
