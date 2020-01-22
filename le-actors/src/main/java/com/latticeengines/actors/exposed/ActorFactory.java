package com.latticeengines.actors.exposed;

import javax.inject.Inject;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.SpringActorProducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinPool;
import akka.routing.RouterConfig;
import akka.routing.SmallestMailboxPool;

@Component
public class ActorFactory {
    @Inject
    private ApplicationContext applicationContext;

    public ActorRef create(ActorSystem actorSystem, //
            String actorName, Class<? extends UntypedActor> actorClazz) {
        Props props = createProps(actorClazz);
        return actorSystem.actorOf(props, actorName);
    }

    public ActorRef create(ActorSystem actorSystem, //
            String actorName, Class<? extends UntypedActor> actorClazz, //
            RoutingLogic routingLogic, int actorCount) {
        if (actorCount == 1) {
            return create(actorSystem, actorName, actorClazz);
        }

        RouterConfig logic = null;
        switch (routingLogic) {
        case SmallestMailboxRoutingLogic:
            logic = new SmallestMailboxPool(actorCount);
            break;
        case RoundRobinRoutingLogic:
            logic = new RoundRobinPool(actorCount);
            break;
        default:
            logic = new RoundRobinPool(actorCount);
        }

        Props props = createProps(actorClazz)//
                .withRouter(logic);
        return actorSystem.actorOf(props, actorName);
    }

    private Props createProps(Class<? extends UntypedActor> actorClazz) {
        return Props.create(//
                SpringActorProducer.class, //
                applicationContext, //
                actorClazz);
    }
}
