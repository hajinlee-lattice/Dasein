package com.latticeengines.cache.configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.aws.elasticache.ElastiCacheService;
import com.latticeengines.domain.exposed.cache.CacheName;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;

@Configuration
@EnableCaching
public class LettuceCacheBeansConfiguration implements CachingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(LettuceCacheBeansConfiguration.class);

    @Inject
    private ElastiCacheService elastiCacheService;

    @Value("${cache.type}")
    private String cacheType;

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
        long ttl = 2;
        RedisCacheConfiguration sessionCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofMinutes(20)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.SessionCacheName));

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
        RedisCacheConfiguration entityCountCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.EntityCountCacheName));
        RedisCacheConfiguration entityDataCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.EntityDataCacheName));
        RedisCacheConfiguration entityRatingCountCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.EntityRatingCountCacheName));
        RedisCacheConfiguration ratingCoverageCacheConfig = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofDays(ttl)) //
                .disableCachingNullValues() //
                .serializeKeysWith(SerializationPair.fromSerializer(new StringRedisSerializer())) //
                .serializeValuesWith(SerializationPair.fromSerializer(getValueSerializer())) //
                .prefixKeysWith(getPrefix(CacheName.Constants.RatingCoverageCacheName));
        // =====================
        // END: objectapi proxy
        // =====================

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

        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();
        cacheConfigs.put(CacheName.Constants.SessionCacheName, sessionCacheConfig);
        cacheConfigs.put(CacheName.Constants.DataLakeCMCacheName, dataLakeCMCacheConfig);
        cacheConfigs.put(CacheName.Constants.DataLakeTopNTreeCache, dataLakeTopNTreeCache);
        cacheConfigs.put(CacheName.Constants.DataLakeStatsCubesCache, dataLakeStatsCacheConfig);

        cacheConfigs.put(CacheName.Constants.EntityCountCacheName, entityCountCacheConfig);
        cacheConfigs.put(CacheName.Constants.EntityDataCacheName, entityDataCacheConfig);
        cacheConfigs.put(CacheName.Constants.EntityRatingCountCacheName, entityRatingCountCacheConfig);
        cacheConfigs.put(CacheName.Constants.RatingCoverageCacheName, ratingCoverageCacheConfig);
        cacheConfigs.put(CacheName.Constants.TableRoleMetadataCacheName, tableRoleMetadataCacheConfig);

        cacheConfigs.put(CacheName.Constants.DataCloudVersionCacheName, dataCloudVersionCacheConfig);

        RedisCacheManager cacheManager = RedisCacheManager
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory()))//
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
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory()))//
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

    @Bean
    public RedisConnectionFactory lettuceConnectionFactory() {
        LedpMasterSlaveConfiguration masterSlave = new LedpMasterSlaveConfiguration(
                elastiCacheService.getNodeAddresses().stream().map(RedisURI::create).collect(Collectors.toList()));

        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .readFrom(ReadFrom.SLAVE_PREFERRED)//
                .commandTimeout(Duration.ofMinutes(1))//
                .shutdownTimeout(Duration.ZERO) //
                .useSsl() //
                .and() //
                .build();

        LedpLettuceConnectionFactory factory = new LedpLettuceConnectionFactory(masterSlave, clientConfig);
        factory.afterPropertiesSet();
        return factory;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(getValueSerializer());
        redisTemplate.setConnectionFactory(lettuceConnectionFactory());
        redisTemplate.setEnableTransactionSupport(true);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
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
