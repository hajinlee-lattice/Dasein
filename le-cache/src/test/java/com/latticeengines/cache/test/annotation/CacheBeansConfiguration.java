package com.latticeengines.cache.test.annotation;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.redisson.api.RedissonClient;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonSpringCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.latticeengines.cache.configuration.LedpLettuceConnectionFactory;
import com.latticeengines.cache.configuration.LedpMasterSlaveConfiguration;
import com.latticeengines.cache.configuration.LettuceCacheBeansConfiguration;
import com.latticeengines.domain.exposed.cache.CacheName;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.Utf8StringCodec;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;

@Configuration
@EnableCaching
public class CacheBeansConfiguration {

//    @Autowired
//    private RedissonClient redisson;

    @Bean
    //@DependsOn("redisson")
    public CacheManager redisTestCacheManager() {

//        CacheConfig testCacheConfig = new CacheConfig(5 * 1000, 15 * 60 * 1000);
//
//        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
//        config.put("Test", testCacheConfig);
//
//        return new RedissonSpringCacheManager(redisson, config);
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()//
                .entryTtl(Duration.ofSeconds(1)) //
                .prefixKeysWith("a")
                .disableCachingNullValues();

        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();
        cacheConfigs.put(CacheName.SessionCache.name(), RedisCacheConfiguration.defaultCacheConfig()//
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
                Arrays.asList(RedisURI.create("rediss://qa1-encrypted-001.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
                        RedisURI.create("rediss://qa1-encrypted-002.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
                                RedisURI.create("rediss://qa1-encrypted-003.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com") //
                        ));
        
        
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .readFrom(ReadFrom.SLAVE)//
                
                .useSsl() //
                .and() //
                .build();
        
        LedpLettuceConnectionFactory factory = new LedpLettuceConnectionFactory(masterSlave, clientConfig);

        factory.afterPropertiesSet();
        return factory;
    }
    
    @Bean
    public RedisTemplate<Object, Object> redisTemplate() {
        RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(LettuceCacheBeansConfiguration.getValueSerializer());
        redisTemplate.setConnectionFactory(lettuceConnectionFactory());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        RedisClient redisClient = RedisClient.create();

        List<RedisURI> nodes = Arrays.asList(RedisURI.create("rediss://qa1-encrypted-001.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
                RedisURI.create("rediss://qa1-encrypted-002.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
                RedisURI.create("rediss://qa1-encrypted-003.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"));

        StatefulRedisMasterSlaveConnection<String, String> connection = MasterSlave
                .connect(redisClient, new Utf8StringCodec(), nodes);
        connection.setReadFrom(ReadFrom.SLAVE);
        connection.async().set("foo", "bar");
        connection.reactive().flushall();
        System.out.println("Connected to Redis");
        System.out.println(connection.async().get("foo").get());
        connection.close();
        redisClient.shutdown();
        
        // Syntax: redis://[password@]host[:port][/databaseNumber]
//        LedpMasterSlaveConfiguration masterSlave = new LedpMasterSlaveConfiguration(
//                Arrays.asList(RedisURI.create("rediss://qa1-encrypted-001.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
//                        RedisURI.create("rediss://qa1-encrypted-002.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com"),
//                                RedisURI.create("rediss://qa1-encrypted-003.qa1-encrypted.wqkmsp.use1.cache.amazonaws.com")));
//        
//        
//        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
//                .readFrom(ReadFrom.SLAVE_PREFERRED)//
//                .commandTimeout(Duration.ofSeconds(2))//
//                .shutdownTimeout(Duration.ofMinutes(1)) //
//                .useSsl() //
//                .and() //
//                .build();
//        
//        LedpLettuceConnectionFactory factory = new LedpLettuceConnectionFactory(masterSlave, clientConfig);
//
//        factory.afterPropertiesSet();
//         connection = factory.getConnection();
//        connection.set("foo".getBytes(), "bar".getBytes());
//
//        System.out.println(new String(connection.get("foo".getBytes())));
//
//        connection.close();
        
    }
}
