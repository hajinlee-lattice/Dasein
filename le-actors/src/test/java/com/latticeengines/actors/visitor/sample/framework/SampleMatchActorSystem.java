package com.latticeengines.actors.visitor.sample.framework;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorFactory;
import com.latticeengines.actors.exposed.RoutingLogic;
import com.latticeengines.actors.visitor.sample.impl.SampleDnbLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDynamoLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleFuzzyMatchAnchorActor;
import com.latticeengines.actors.visitor.sample.impl.SampleLocationToDunsMicroEngineActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

@Component("sampleMatchActorSystem")
public class SampleMatchActorSystem {

    private static final Logger log = LoggerFactory.getLogger(SampleMatchActorSystem.class);

    public static final int ACTOR_CARDINALITY = 5;

    private ActorSystem system;

    @Autowired
    private ActorFactory actorFactory;

    private ConcurrentMap<String, ActorRef> actorRefMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void postConstruct() {
        Config config = ConfigFactory.load();
        system = ActorSystem.create("SampleFuzzyMatch", config.getConfig("akka"));
        log.info("Actor system for match started");
        initActors();
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Shutting down match actor system");
        system.shutdown();
        log.info("Completed shutdown of match actor system");
    }

    public <T extends ActorTemplate> ActorRef getActorRef(Class<T> actorClz) {
        return actorRefMap.get(actorClz.getCanonicalName());
    }

    public ActorRef getFuzzyMatchAnchor() {
        return getActorRef(SampleFuzzyMatchAnchorActor.class);
    }

    public void sendResponse(Object response, String returnAddress) {
        ActorRef ref = system.actorFor(returnAddress);
        ref.tell(response, null);
    }

    private void initActors() {
        initNamedActor(SampleDynamoLookupActor.class, true);
        initNamedActor(SampleDnbLookupActor.class, true);

        initMicroEngines();

        initNamedActor(SampleFuzzyMatchAnchorActor.class);

        log.info("All match actors started");
    }

    private void initMicroEngines() {
        initNamedActor(SampleDunsDomainBasedMicroEngineActor.class);
        initNamedActor(SampleDomainBasedMicroEngineActor.class);
        initNamedActor(SampleDunsBasedMicroEngineActor.class);
        initNamedActor(SampleLocationToDunsMicroEngineActor.class);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz) {
        return initNamedActor(actorClz, false);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting) {
        ActorRef actorRef = null;
        if (useRouting) {
            actorRef = actorFactory.create(system, actorClz.getSimpleName(), actorClz,
                    RoutingLogic.RoundRobinRoutingLogic, ACTOR_CARDINALITY);
        } else {
            actorRef = actorFactory.create(system, actorClz.getSimpleName(), actorClz);
        }
        actorRefMap.put(actorClz.getCanonicalName(), actorRef);
        log.info("Add actor-ref " + actorClz.getSimpleName() + " to actorRefMap.");
        return actorRef;
    }

}
