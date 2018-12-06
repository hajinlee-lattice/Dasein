package com.latticeengines.actors.utils;

import akka.actor.ActorRef;

public class ActorUtils {
    public static String getPath(ActorRef actorRef) {
        return actorRef.path().toSerializationFormat();
    }
}
