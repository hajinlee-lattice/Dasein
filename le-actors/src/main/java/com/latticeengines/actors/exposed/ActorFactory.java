package com.latticeengines.actors.exposed;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.SpringActorProducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

@Component
public class ActorFactory {
    @Autowired
    private ApplicationContext applicationContext;

    public ActorRef create(ActorSystem actorSystem, //
            String actorName, Class<? extends UntypedActor> actorClazz) {
        Props props = Props.create(//
                SpringActorProducer.class, //
                applicationContext, //
                actorClazz);
        return actorSystem.actorOf(props, actorName);
    }
}
