package com.latticeengines.cache.test.annotation;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;

import com.latticeengines.aws.elasticache.ElastiCacheService;
import com.latticeengines.cache.configuration.LedpLettuceConnectionFactory;
import com.latticeengines.cache.configuration.LedpMasterSlaveConfiguration;
import com.latticeengines.domain.exposed.cache.CacheName;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;

@Configuration
@EnableCaching
public class CacheBeansConfiguration {

    @Inject
    private ElastiCacheService elastiCacheService;

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
        RedisCacheManager cacheManager = RedisCacheManager
                .builder(RedisCacheWriter.lockingRedisCacheWriter(lettuceConnectionFactory()))//
                .cacheDefaults(config) //
                .withInitialCacheConfigurations(cacheConfigs) //
                .transactionAware()//
                .build();
        return cacheManager;
    }

    @Bean
    public RedisConnectionFactory lettuceConnectionFactory() {
        LedpMasterSlaveConfiguration masterSlave = new LedpMasterSlaveConfiguration(
                elastiCacheService.getNodeAddresses().stream().map(RedisURI::create).collect(Collectors.toList()));

        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .readFrom(ReadFrom.SLAVE_PREFERRED)//
                .commandTimeout(Duration.ofSeconds(2))//
                .shutdownTimeout(Duration.ZERO) //
                .useSsl() //
                .and() //
                .build();

        LedpLettuceConnectionFactory factory = new LedpLettuceConnectionFactory(masterSlave, clientConfig);
        factory.afterPropertiesSet();
        return factory;
    }
}
