package com.latticeengines.camille.exposed.messaging;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;

public class MessageQueueFactory {

    public static MessageQueueFactory instance() {
        if (instance == null) {
            synchronized (MessageQueueFactory.class) {
                if (instance == null) {
                    instance = new MessageQueueFactory();
                }
            }
        }
        return instance;
    }

    private MessageQueueFactory() {
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> MessageQueue<T> construct(Class<T> messageClazz, String queueName,
            MessageConsumer<T> consumer) {

        Path path;
        try {
            path = constructPath(queueName);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Queue name %s is invalid: %s", queueName, e.getMessage()), e);
        }
        MessageQueue<T> existing = (MessageQueue<T>) queues.get(path);

        if (existing != null) {
            // Can't have different message types on the same queue
            if (!existing.getMessageClazz().equals(messageClazz)) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot have different message types on the same queue.  An existing queue has already been constructed with queue name %s but with a message type %s.  Attempted to create queue with message type %s",
                                queueName, existing.getMessageClazz(), messageClazz));
            }
            return existing;
        } else {
            MessageQueue<T> queue = new MessageQueue<T>(path, messageClazz, consumer);
            queues.put(path, queue);
            return queue;
        }
    }

    /**
     * For unit testing only
     */
    public synchronized void clearCache() {
        queues.clear();
    }

    private Path constructPath(String queueName) {
        return PathBuilder.buildMessageQueuePath(CamilleEnvironment.getPodId(), queueName);
    }

    private Map<Path, Object> queues = new HashMap<Path, Object>();
    private static MessageQueueFactory instance;
}
