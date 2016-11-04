package com.latticeengines.datacloud.match.actors.framework;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorFactory;
import com.latticeengines.actors.exposed.TimerMessage;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.FiniteDuration;

@Component("matchActorSystem")
public class MatchActorSystem {

    private static final Log log = LogFactory.getLog(MatchActorSystem.class);

    private static final String BATCH_MODE = "batch";
    private static final String REALTIME_MODE = "realtime";

    private static final int MAX_ALLOWED_RECORD_COUNT_SYNC = 200;
    private static final int MAX_ALLOWED_RECORD_COUNT_ASYNC = 10000;
    private final AtomicInteger maxAllowedRecordCount = new AtomicInteger(MAX_ALLOWED_RECORD_COUNT_SYNC);

    private ActorSystem system;

    private boolean batchMode = false;

    @Autowired
    private ActorFactory actorFactory;

    private ConcurrentMap<String, ActorRef> actorRefMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void postConstruct() {
        Config config = ConfigFactory.load();
        system = ActorSystem.create("FuzzyMatch", config.getConfig("akka"));
        log.info("Actor system for match started");
        initActors();
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Shutting down match actor system");
        system.shutdown();
        log.info("Completed shutdown of match actor system");
    }

    public void registerTimer(Class<? extends ActorTemplate> actorClazz, //
            int timerFrequency, TimeUnit timeUnit, TimerMessage timerMessage) {
        system.scheduler().schedule(//
                FiniteDuration.create(0, TimeUnit.MILLISECONDS), //
                FiniteDuration.create(timerFrequency, timeUnit), //
                getActorRef(actorClazz), //
                timerMessage, //
                system.dispatcher(), //
                null);
    }

    public <T extends ActorTemplate> ActorRef getActorRef(Class<T> actorClz) {
        return actorRefMap.get(actorClz.getCanonicalName());
    }

    public ActorRef getFuzzyMatchAnchor() {
        return getActorRef(FuzzyMatchAnchorActor.class);
    }

    public void sendResponse(Object response, String returnAddress) {
        ActorRef ref = system.actorFor(returnAddress);
        ref.tell(response, null);
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
        if (batchMode) {
            maxAllowedRecordCount.set(MAX_ALLOWED_RECORD_COUNT_ASYNC);
        } else {
            maxAllowedRecordCount.set(MAX_ALLOWED_RECORD_COUNT_SYNC);
        }
        log.info("Switch MatchActorSystem to " + (isBatchMode() ? BATCH_MODE : REALTIME_MODE) + " mode.");
    }

    public int getMaxAllowedRecordCount() {
        return maxAllowedRecordCount.get();
    }

    private void initActors() {
        initNamedActor(DynamoLookupActor.class);
        initNamedActor(DnbLookupActor.class);

        initMicroEngines();

        initNamedActor(FuzzyMatchAnchorActor.class);

        log.info("All match actors started");
    }

    private void initMicroEngines() {
        initNamedActor(DunsDomainBasedMicroEngineActor.class);
        initNamedActor(DomainBasedMicroEngineActor.class);
        initNamedActor(DunsBasedMicroEngineActor.class);
        initNamedActor(LocationToDunsMicroEngineActor.class);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz) {
        ActorRef actorRef = actorFactory.create(system, actorClz.getSimpleName(), actorClz);
        actorRefMap.put(actorClz.getCanonicalName(), actorRef);
        log.info("Add actor-ref " + actorClz.getSimpleName() + " to actorRefMap.");
        return actorRef;
    }

}
