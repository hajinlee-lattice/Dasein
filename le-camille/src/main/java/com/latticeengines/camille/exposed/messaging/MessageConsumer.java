package com.latticeengines.camille.exposed.messaging;

public abstract class MessageConsumer<T> {
    /**
     * If throws, framework code will log and message will be consumed as usual.
     * 
     * @param message
     */
    public abstract void consume(T message);
}
