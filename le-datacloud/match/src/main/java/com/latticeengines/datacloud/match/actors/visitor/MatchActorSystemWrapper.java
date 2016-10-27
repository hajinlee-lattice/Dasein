//package com.latticeengines.datacloud.match.actors.visitor;
//
//import javax.annotation.PostConstruct;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
//import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
//import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
//import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
//import com.latticeengines.datacloud.match.actors.visitor.impl.LocationBasedMicroEngineActor;
//import com.latticeengines.datacloud.match.actors.visitor.impl.MatchActor;
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;
//
//import akka.actor.ActorRef;
//import akka.actor.ActorSystem;
//import akka.actor.Props;
//
//@Component
//public class MatchActorSystemWrapper {
//    private static final Log log = LogFactory.getLog(MatchActorSystemWrapper.class);
//
//    private ActorSystem system;
//    private ActorRef matchActor;
//    private MatchActorStateTransitionGraph matchActorStateTransitionGraph;
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
//    public ActorRef getMatchActor() {
//        return matchActor;
//    }
//
//    public MatchGuideBook createGuideBook() {
//        return new MatchGuideBook(matchActorStateTransitionGraph);
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
//                system.actorOf(Props.create(DynamoLookupActor.class), //
//                        "dynamoLookupActor");
//
//        ActorRef dnbLookupActor = //
//                system.actorOf(Props.create(DynamoLookupActor.class), //
//                        "dnbLookupActor");
//
//        ActorRef dunsDomainBasedMicroEngineActor = //
//                system.actorOf(Props.create(DunsDomainBasedMicroEngineActor.class), //
//                        "dunsDomainBasedMicroEngineActor");
//
//        ActorRef domainBasedMicroEngineActor = //
//                system.actorOf(Props.create(DomainBasedMicroEngineActor.class), //
//                        "domainBasedMicroEngineActor");
//
//        ActorRef microEngine3Actor = //
//                system.actorOf(Props.create(DunsBasedMicroEngineActor.class), //
//                        "dunsBasedMicroEngineActor");
//
//        ActorRef microEngine4Actor = //
//                system.actorOf(Props.create(LocationBasedMicroEngineActor.class), //
//                        "locationBasedMicroEngineActor");
//
//        matchActor = //
//                system.actorOf(Props.create(MatchActor.class), //
//                        "matchActor");
//
//        matchActorStateTransitionGraph = new MatchActorStateTransitionGraph(matchActor, //
//                dunsDomainBasedMicroEngineActor, dynamoLookupActor, domainBasedMicroEngineActor, //
//                dynamoLookupActor, microEngine3Actor, dynamoLookupActor, //
//                microEngine4Actor, dnbLookupActor, dunsDomainBasedMicroEngineActor, dynamoLookupActor);
//
//        log.info("All match actors started");
//    }
//}
