package com.latticeengines.datacloud.match.actors.visitor;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationBasedMicroEngineActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

@Component
public class MatchActorSystemWrapper {
    private static final Log log = LogFactory.getLog(MatchActorSystemWrapper.class);

    private ActorSystem system;
    private ActorRef fuzzyMatchAnchor;
    private MatchActorStateTransitionGraph matchActorStateTransitionGraph;

    @PostConstruct
    public void init() {
        Config config = ConfigFactory.load();
        system = ActorSystem.create("FuzzyMatch", config.getConfig("akka"));
        log.info("Actor system for match started");

        initActors();
    }

    public ActorRef getFuzzyMatchAnchor() {
        return fuzzyMatchAnchor;
    }

    public MatchGuideBook createGuideBook() {
        return new MatchGuideBook(matchActorStateTransitionGraph);
    }

    public void shutdown() {
        log.info("Shutting down match actor system");
        system.shutdown();
        log.info("Completed shutdown of match actor system");
    }

    private void initActors() {
        ActorRef dynamoLookupActor = //
                system.actorOf(Props.create(DynamoLookupActor.class), //
                        "dynamoLookupActor");

        ActorRef dnbLookupActor = //
                system.actorOf(Props.create(DnbLookupActor.class), //
                        "dnbLookupActor");

        ActorRef dunsDomainBasedMicroEngineActor = //
                system.actorOf(Props.create(DunsDomainBasedMicroEngineActor.class), //
                        "dunsDomainBasedMicroEngineActor");

        ActorRef domainBasedMicroEngineActor = //
                system.actorOf(Props.create(DomainBasedMicroEngineActor.class), //
                        "domainBasedMicroEngineActor");

        ActorRef microEngine3Actor = //
                system.actorOf(Props.create(DunsBasedMicroEngineActor.class), //
                        "dunsBasedMicroEngineActor");

        ActorRef microEngine4Actor = //
                system.actorOf(Props.create(LocationBasedMicroEngineActor.class), //
                        "locationBasedMicroEngineActor");

        fuzzyMatchAnchor = //
                system.actorOf(Props.create(FuzzyMatchAnchorActor.class), //
                        "fuzzyMatchAnchorActor");

        matchActorStateTransitionGraph = new MatchActorStateTransitionGraph(
                dunsDomainBasedMicroEngineActor.path().toSerializationFormat(),
                domainBasedMicroEngineActor.path().toSerializationFormat(),
                microEngine3Actor.path().toSerializationFormat(), //
                microEngine4Actor.path().toSerializationFormat(),
                dunsDomainBasedMicroEngineActor.path().toSerializationFormat());

        Map<String, String> dataSourceActors = new HashMap<>();
        dataSourceActors.put("dynamo", dynamoLookupActor.path().toSerializationFormat());
        dataSourceActors.put("dnb", dnbLookupActor.path().toSerializationFormat());

        matchActorStateTransitionGraph.setDataSourceActors(dataSourceActors);
        log.info("All match actors started");
    }

    public static void sendResponse(Object system, Object response, String returnAddress) {
        ActorSystem actorSystem = (ActorSystem) system;
        ActorRef ref = actorSystem.actorFor(returnAddress);
        ref.tell(response, null);
    }
}
