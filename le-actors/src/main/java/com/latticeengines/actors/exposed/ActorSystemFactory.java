package com.latticeengines.actors.exposed;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;

public final class ActorSystemFactory {

    protected ActorSystemFactory() {
        throw new UnsupportedOperationException();
    }

    public static ActorSystem create(String name, int numDispatchers) {
        Config akkaConf = ConfigFactory.parseString(String.format("akka {\n" +
                "  actor {\n" +
                "    default-dispatcher {\n" +
                "      throughput = %d\n" +
                "    }\n" +
                "  }\n" +
                "}", numDispatchers));
        return ActorSystem.create(name, ConfigFactory.load(akkaConf));
    }

}
