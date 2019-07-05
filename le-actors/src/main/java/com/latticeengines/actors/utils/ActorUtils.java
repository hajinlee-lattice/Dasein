package com.latticeengines.actors.utils;

import akka.actor.ActorRef;

public class ActorUtils {

    public static final String INJECTED_FAILURE_MSG = "Failure injected";

    public static String getPath(ActorRef actorRef) {
        return actorRef.path().toSerializationFormat();
    }
}
