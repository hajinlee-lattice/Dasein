package com.latticeengines.redis.util;

import java.time.Duration;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.lettuce.core.RedisURI;

public final class RedisTemplateUtils {

    protected RedisTemplateUtils() {
        throw new UnsupportedOperationException();
    }

    public static RedisTemplate<String, Object> createRedisTemplate(boolean localRedis, int timeout,
                                                                    String redisEndpoint) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(getValueSerializer());
        redisTemplate.setConnectionFactory(lettuceConnectionFactory(localRedis, timeout, redisEndpoint));
        redisTemplate.setEnableTransactionSupport(true);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    public static RedisTemplate<String, Object> createPoolingRedisTemplate(boolean localRedis, int timeout,
                                                                    String redisEndpoint) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(getValueSerializer());
        redisTemplate.setConnectionFactory(poolingLettuceConnectionFactory(localRedis, timeout, redisEndpoint));
        redisTemplate.setEnableTransactionSupport(true);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    private static RedisConnectionFactory poolingLettuceConnectionFactory(boolean localRedis, int redisTimeout,
                                                                   String redisEndpoint) {
        RedisConnectionFactory factory;
        if (localRedis) {
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            LettucePoolingClientConfiguration poolingClientConfiguration = LettucePoolingClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, poolingClientConfiguration);
        } else {
            RedisURI redisURI = RedisURI.create(redisEndpoint);
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            standaloneConfiguration.setHostName(redisURI.getHost());
            LettucePoolingClientConfiguration poolingClientConfig = LettucePoolingClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .useSsl() //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, poolingClientConfig);
        }
        ((LettuceConnectionFactory) factory).afterPropertiesSet();
        return factory;
    }

    private static RedisConnectionFactory lettuceConnectionFactory(boolean localRedis, int redisTimeout,
                                                                   String redisEndpoint) {
        RedisConnectionFactory factory;
        if (localRedis) {
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, clientConfig);
        } else {
            RedisURI redisURI = RedisURI.create(redisEndpoint);
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            standaloneConfiguration.setHostName(redisURI.getHost());
            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                    .commandTimeout(Duration.ofMinutes(redisTimeout))//
                    .shutdownTimeout(Duration.ZERO) //
                    .useSsl() //
                    .build();
            factory = new LettuceConnectionFactory(standaloneConfiguration, clientConfig);
        }
        ((LettuceConnectionFactory) factory).afterPropertiesSet();
        return factory;
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
