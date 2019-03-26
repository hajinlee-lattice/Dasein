package com.latticeengines.redis.util;

import java.time.Duration;

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

import io.lettuce.core.RedisURI;

public class RedisTemplateUtils {

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
