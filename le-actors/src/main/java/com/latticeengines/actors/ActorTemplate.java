package com.latticeengines.actors;

import akka.actor.UntypedActor;

public abstract class ActorTemplate extends UntypedActor {
    protected abstract boolean isValidMessageType(Object msg);

    protected abstract void processMessage(Object msg);

    @Override
    public void onReceive(Object msg) {
        if (isValidMessageType(msg)) {
            processMessage(msg);
        } else {
            unhandled(msg);
        }
    }
}