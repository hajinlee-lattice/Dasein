//package com.latticeengines.actors.visitor.sample;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import javax.annotation.PostConstruct;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.actors.visitor.sample.impl.SampleDnbLookupActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleDomainBasedMicroEngineActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleDunsBasedMicroEngineActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleDunsDomainBasedMicroEngineActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleDynamoLookupActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleFuzzyMatchAnchorActor;
//import com.latticeengines.actors.visitor.sample.impl.SampleLocationBasedMicroEngineActor;
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;
//
//import akka.actor.ActorRef;
//import akka.actor.ActorSystem;
//import akka.actor.Props;
//
//@Component
//public class SampleMatchActorSystemWrapper {
//    private static final Log log = LogFactory.getLog(SampleMatchActorSystemWrapper.class);
//
//    private ActorSystem system;
//    private ActorRef fuzzyMatchAnchor;
//    private SampleMatchActorStateTransitionGraph matchActorStateTransitionGraph;
//
//    @PostConstruct
//    public void init() {
//        Config config = ConfigFactory.load();
//        system = ActorSystem.create("FuzzyMatch", config.getConfig("akka"));
//        log.info("Actor system for match started");
//
//        initActors();
//    }
//
//    public ActorRef getFuzzyMatchAnchor() {
//        return fuzzyMatchAnchor;
//    }
//
//    public SampleMatchGuideBook createGuideBook() {
//        return new SampleMatchGuideBook(matchActorStateTransitionGraph);
//    }
//
//    public void shutdown() {
//        log.info("Shutting down match actor system");
//        system.shutdown();
//        log.info("Completed shutdown of match actor system");
//    }
//
//    private void initActors() {
//        ActorRef dynamoLookupActor = //
//                system.actorOf(Props.create(SampleDynamoLookupActor.class), //
//                        "dynamoLookupActor");
//
//        ActorRef dnbLookupActor = //
//                system.actorOf(Props.create(SampleDnbLookupActor.class), //
//                        "dnbLookupActor");
//
//        ActorRef dunsDomainBasedMicroEngineActor = //
//                system.actorOf(Props.create(SampleDunsDomainBasedMicroEngineActor.class), //
//                        "dunsDomainBasedMicroEngineActor");
//
//        ActorRef domainBasedMicroEngineActor = //
//                system.actorOf(Props.create(SampleDomainBasedMicroEngineActor.class), //
//                        "domainBasedMicroEngineActor");
//
//        ActorRef microEngine3Actor = //
//                system.actorOf(Props.create(SampleDunsBasedMicroEngineActor.class), //
//                        "dunsBasedMicroEngineActor");
//
//        ActorRef microEngine4Actor = //
//                system.actorOf(Props.create(SampleLocationBasedMicroEngineActor.class), //
//                        "locationBasedMicroEngineActor");
//
//        fuzzyMatchAnchor = //
//                system.actorOf(Props.create(SampleFuzzyMatchAnchorActor.class), //
//                        "fuzzyMatchAnchorActor");
//
//        matchActorStateTransitionGraph = new SampleMatchActorStateTransitionGraph(
//                dunsDomainBasedMicroEngineActor.path().toSerializationFormat(),
//                domainBasedMicroEngineActor.path().toSerializationFormat(),
//                microEngine3Actor.path().toSerializationFormat(), //
//                microEngine4Actor.path().toSerializationFormat(),
//                dunsDomainBasedMicroEngineActor.path().toSerializationFormat());
//
//        Map<String, String> dataSourceActors = new HashMap<>();
//        dataSourceActors.put("dynamo", dynamoLookupActor.path().toSerializationFormat());
//        dataSourceActors.put("dnb", dnbLookupActor.path().toSerializationFormat());
//
//        matchActorStateTransitionGraph.setDataSourceActors(dataSourceActors);
//        log.info("All match actors started");
//    }
//
//    public static void sendResponse(Object system, Object response, String returnAddress) {
//        ActorSystem actorSystem = (ActorSystem) system;
//        ActorRef ref = actorSystem.actorFor(returnAddress);
//        ref.tell(response, null);
//    }
//}
