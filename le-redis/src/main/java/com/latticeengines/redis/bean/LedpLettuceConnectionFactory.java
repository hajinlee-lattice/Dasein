package com.latticeengines.redis.bean;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

public class LedpLettuceConnectionFactory extends LettuceConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(LedpLettuceConnectionFactory.class);

    private final LedpMasterSlaveConfiguration configuration;
    private final LettuceClientConfiguration clientConfiguration;

    public LedpLettuceConnectionFactory(LedpMasterSlaveConfiguration standaloneConfig,
                                        LettuceClientConfiguration clientConfig) {
        super(standaloneConfig, clientConfig);
        this.configuration = standaloneConfig;
        this.clientConfiguration = clientConfig;
    }

    @Override
    protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client, RedisCodec<?, ?> codec) {
        return new ElasticacheConnectionProvider((RedisClient) client, codec, getClientConfiguration().getReadFrom(),
                this.configuration, this.clientConfiguration);
    }

    static class ElasticacheConnectionProvider implements LettuceConnectionProvider, TargetAware {

        private final RedisClient client;
        private final RedisCodec<?, ?> codec;
        private final Optional<ReadFrom> readFrom;
        private final LedpMasterSlaveConfiguration configuration;
        private final LettuceClientConfiguration clientConfiguration;

        ElasticacheConnectionProvider(RedisClient client, RedisCodec<?, ?> codec, Optional<ReadFrom> readFrom,
                LedpMasterSlaveConfiguration configuration, LettuceClientConfiguration clientConfiguration) {

            this.client = client;
            this.codec = codec;
            this.readFrom = readFrom;
            this.configuration = configuration;
            this.clientConfiguration = clientConfiguration;

            Duration duration = clientConfiguration.getCommandTimeout();
            log.info("Set redis client default command timeout to " + duration);
            this.client.setDefaultTimeout(duration);
        }

        @Override
        public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

            if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
                return connectionType.cast(client.connectSentinel());
            }

            if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
                return connectionType.cast(client.connectPubSub(codec));
            }

            if (StatefulConnection.class.isAssignableFrom(connectionType)) {
                return connectionType
                        .cast(readFrom.map(it -> this.masterSlaveConnection(configuration.getEndpoints(), it))
                                .orElseGet(() -> client.connect(codec)));
            }

            throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
        }

        @Override
        public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType, RedisURI redisURI) {

            if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
                return connectionType.cast(client.connectSentinel(redisURI));
            }

            if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
                return connectionType.cast(client.connectPubSub(codec, redisURI));
            }

            if (StatefulConnection.class.isAssignableFrom(connectionType)) {
                if (readFrom.isPresent()) {
                    throw new UnsupportedOperationException(
                            "Not supported. Configured to Master/Slave with multiple URLs");
                }
                return connectionType.cast(client.connect(codec));
            }

            throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
        }

        @SuppressWarnings("rawtypes")
        private StatefulRedisConnection masterSlaveConnection(List<RedisURI> endpoints, ReadFrom readFrom) {
            StatefulRedisMasterSlaveConnection<?, ?> connection = MasterSlave.connect(client, codec, endpoints);
            connection.setReadFrom(readFrom);
            Duration duration = clientConfiguration.getCommandTimeout();
            log.info("Set ElasticCache master slave connection command timeout to " + duration);
            connection.setTimeout(duration);
            return connection;
        }
    }
}
