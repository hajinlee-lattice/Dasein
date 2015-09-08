package com.latticeengines.camille.exposed.messaging;

import java.nio.charset.StandardCharsets;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Path;

public class MessageQueue<T> {

    /**
     * Do not invoke directly. Instead use MessageQueueFactory
     */
    public MessageQueue(Path path, Class<T> messageClazz, MessageConsumer<T> consumer) {
        this.messageClazz = messageClazz;
        this.path = path;
        try {
            Camille c = CamilleEnvironment.getCamille();
            c.upsert(path, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            QueueBuilder<T> builder = QueueBuilder.builder(c.getCuratorClient(), consumer != null ? new Consumer<T>(
                    consumer, path) : null, new Serializer<T>(messageClazz), path.toString());
            this.inner = builder.buildQueue();
            this.inner.start();
        } catch (Exception e) {
            String error = String.format("Error starting queue %s: %s", path, e.getMessage());
            log.error(error, e);
            throw new RuntimeException(error, e);
        }
    }

    public void put(T message) {
        try {
            inner.put(message);
        } catch (Exception e) {
            String error = String.format("Failed to place message %s on queue %s: %s", message, path, e.getMessage());
            log.error(error, e);
            throw new RuntimeException(error, e);
        }

    }

    private static class Consumer<M> implements QueueConsumer<M> {

        public Consumer(MessageConsumer<M> inner, Path path) {
            this.inner = inner;
            this.path = path;
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            if (newState == ConnectionState.CONNECTED || newState == ConnectionState.RECONNECTED) {
                log.info("State of queue {} changed to {}", path, newState);
            } else if (newState == ConnectionState.LOST || newState == ConnectionState.READ_ONLY
                    || newState == ConnectionState.SUSPENDED) {
                log.error("State of queue {} changed to {}", path, newState);
            }
        }

        @Override
        public void consumeMessage(M message) throws Exception {
            log.info("Received message {} on queue {}", message, path);
            try {
                inner.consume(message);
            } catch (Exception e) {
                String error = String.format("Unhandled exception caught processing message %s on queue %s", message,
                        path);
                log.error(error, e);
                throw new RuntimeException(error, e);
            }
        }

        private final MessageConsumer<M> inner;
        private final Path path;
    }

    private static class Serializer<M> implements QueueSerializer<M> {
        public Serializer(Class<M> messageClazz) {
            this.messageClazz = messageClazz;
        }

        @Override
        public byte[] serialize(M item) {
            String json = JsonUtils.serialize(item);
            return json.getBytes();
        }

        @Override
        public M deserialize(byte[] bytes) {
            String json = new String(bytes, StandardCharsets.UTF_8);
            M message = JsonUtils.deserialize(json, messageClazz);
            return message;
        }

        private Class<M> messageClazz;
    }

    public Class<T> getMessageClazz() {
        return messageClazz;
    }

    public Path getPath() {
        return path;
    }

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private final Path path;

    private final DistributedQueue<T> inner;
    private final Class<T> messageClazz;

}
