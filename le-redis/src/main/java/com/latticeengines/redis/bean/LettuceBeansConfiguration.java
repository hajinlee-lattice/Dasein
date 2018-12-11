package com.latticeengines.redis.bean;

import java.time.Duration;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.aws.elasticache.ElasticCacheService;

import io.lettuce.core.RedisURI;


@Configuration
public class LettuceBeansConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LettuceBeansConfiguration.class);

    @Inject
    private ElasticCacheService elastiCacheService;

    @Value("${cache.type}")
    private String cacheType;

    @Value("${cache.redis.command.timeout.min}")
    private int redisTimeout;

    @Value("${cache.local.redis}")
    private boolean localRedis;

    @Bean
    @DependsOn(value = "placeholderConfigurer")
    public RedisConnectionFactory lettuceConnectionFactory() {
        RedisConnectionFactory factory;

        if (localRedis) {
            log.info("Using local redis server");
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, clientConfig);
        } else {
//            LedpMasterSlaveConfiguration masterSlave = new LedpMasterSlaveConfiguration(
//                    elastiCacheService.getDistributedCacheNodeAddresses().stream().map(RedisURI::create).collect(Collectors.toList()));
//
//            ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder() //
//                    .enablePeriodicRefresh(Duration.ofSeconds(10)) //
//                    .enableAllAdaptiveRefreshTriggers() //
//                    .refreshTriggersReconnectAttempts(3) //
//                    .build();
//
//            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
//                    .readFrom(ReadFrom.SLAVE_PREFERRED)//
//                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
//                    .shutdownTimeout(Duration.ZERO) //
//                    .clientOptions(ClusterClientOptions.builder().topologyRefreshOptions(topologyRefreshOptions).build()) //
//                    .useSsl() //
//                    .build();
//
//            LedpLettuceConnectionFactory lettuceFactory = new LedpLettuceConnectionFactory(masterSlave, clientConfig);
//            lettuceFactory.afterPropertiesSet();
//            factory = lettuceFactory;

            RedisURI redisURI = RedisURI.create(elastiCacheService.getPrimaryEndpointAddress());
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            standaloneConfiguration.setHostName(redisURI.getHost());
            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .useSsl() //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, clientConfig);
        }

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

    private static RedisSerializer<?> getValueSerializer() {
        Jackson2JsonRedisSerializer<?> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        return jackson2JsonRedisSerializer;
    }

}
