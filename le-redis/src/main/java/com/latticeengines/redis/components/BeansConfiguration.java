package com.latticeengines.redis.components;

import java.util.HashMap;
import java.util.Map;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
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
public class BeansConfiguration {

    @Bean(name = "redisson")
    public RedissonClient constructRedissonClient() {
        Config config = new Config();
        // needs to clean this up
        config.useReplicatedServers().setRetryInterval(2500).setMasterConnectionPoolSize(3)
                .setMasterConnectionMinimumIdleSize(3).setSlaveConnectionPoolSize(3)
                .setSlaveConnectionMinimumIdleSize(3).setScanInterval(2000)
                .addNodeAddress("redis://qa-001.wqkmsp.0001.use1.cache.amazonaws.com:6379",
                        "redis://qa-002.wqkmsp.0001.use1.cache.amazonaws.com:6379",
                        "redis://qa-003.wqkmsp.0001.use1.cache.amazonaws.com:6379");
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }

    @Bean
    public CacheManager cacheManager(RedissonClient redisson) {
        Map<String, CacheConfig> config = new HashMap<String, CacheConfig>();
        config.put(CacheNames.SessionCache.name(), new CacheConfig(5 * 60 * 1000, 5 * 60 * 1000));
        config.put(CacheNames.EntityCache.name(), new CacheConfig(0, 0));
        return new RedissonSpringCacheManager(redisson, config);
    }

}
