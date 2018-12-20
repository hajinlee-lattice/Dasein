package com.latticeengines.actors.visitor.sample.framework;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.visitor.sample.impl.SampleDnbLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDynamoLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleFuzzyMatchAnchorActor;
import com.latticeengines.actors.visitor.sample.impl.SampleLocationToDunsMicroEngineActor;
import com.latticeengines.domain.exposed.actors.ActorType;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

@Component("sampleMatchActorSystem")
public class SampleMatchActorSystem extends ActorSystemTemplate {

    private static final Logger log = LoggerFactory.getLogger(SampleMatchActorSystem.class);

    public static final int ACTOR_CARDINALITY = 5;

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

    @Override
    public <T extends ActorTemplate> ActorRef getActorRef(Class<T> actorClz) {
        return actorRefMap.get(actorClz.getCanonicalName());
    }

    public ActorRef getFuzzyMatchAnchor() {
        return getActorRef(SampleFuzzyMatchAnchorActor.class);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void sendResponse(Object response, String returnAddress) {
        ActorRef ref = system.actorFor(returnAddress);
        ref.tell(response, null);
    }

    @Override
    protected void initAnchors() {
        initNamedActor(SampleFuzzyMatchAnchorActor.class);
        actorNameToType.put(SampleFuzzyMatchAnchorActor.class.getSimpleName(), ActorType.ANCHOR);
        actorNameAbbrToType.put(
                MatchActorUtils.getShortActorName(SampleFuzzyMatchAnchorActor.class.getSimpleName(), ActorType.ANCHOR),
                ActorType.ANCHOR);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void initMicroEngines() {
        Class<? extends ActorTemplate>[] microEngineClz = new Class[] { //
                SampleDunsDomainBasedMicroEngineActor.class, //
                SampleDomainBasedMicroEngineActor.class, //
                SampleDunsBasedMicroEngineActor.class, //
                SampleLocationToDunsMicroEngineActor.class, //
        };
        for (Class<? extends ActorTemplate> clz : microEngineClz) {
            initNamedActor(clz);
            actorNameToType.put(clz.getSimpleName(), ActorType.MICRO_ENGINE);
            actorNameAbbrToType.put(MatchActorUtils.getShortActorName(clz.getSimpleName(), ActorType.MICRO_ENGINE),
                    ActorType.MICRO_ENGINE);
        }
    }

    @Override
    protected void initAssistantActors() {
        initNamedActor(SampleDynamoLookupActor.class, true, 1);
        initNamedActor(SampleDnbLookupActor.class, true, 1);
    }

    @Override
    public ActorRef getAnchor() {
        return getActorRef(SampleFuzzyMatchAnchorActor.class);
    }

}
