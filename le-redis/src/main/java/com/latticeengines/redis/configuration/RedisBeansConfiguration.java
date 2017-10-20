package com.latticeengines.redis.configuration;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
public class RedisBeansConfiguration {

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

}
