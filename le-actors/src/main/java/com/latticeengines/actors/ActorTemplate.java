package com.latticeengines.actors;

import com.latticeengines.actors.exposed.TimerMessage;

import akka.actor.UntypedActor;

public abstract class ActorTemplate extends UntypedActor {
    /**
     * Safe check whether the message is in valid type
     * 
     * @param msg
     * @return
     */
    protected abstract boolean isValidMessageType(Object msg);

    /**
     * Process message received in mail box
     * 
     * @param msg
     */
    protected abstract void processMessage(Object msg);

    @Override
    public void onReceive(Object msg) {
        if (isValidMessageType(msg)) {
            processMessage(msg);
        } else if (msg instanceof TimerMessage) {
            processTimerMessage((TimerMessage) msg);
        } else {
            unhandled(msg);
        }
    }

    protected void processTimerMessage(TimerMessage msg) {
        // do nothing by default
    }
}