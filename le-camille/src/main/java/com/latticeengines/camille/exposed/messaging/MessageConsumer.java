package com.latticeengines.camille.exposed.messaging;

public abstract class MessageConsumer<T> {
    // If throws, will log and continue
    public abstract void consume(T message);
}
