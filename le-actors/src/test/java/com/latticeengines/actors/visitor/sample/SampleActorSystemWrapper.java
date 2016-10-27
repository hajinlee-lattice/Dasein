package com.latticeengines.actors.visitor.sample;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.impl.SampleActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDynamoLookupActor;
import com.latticeengines.actors.visitor.sample.impl.SampleLocationBasedMicroEngineActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

@Component("sampleActorSystemWrapper")
public class SampleActorSystemWrapper {
    private static final Log log = LogFactory.getLog(SampleActorSystemWrapper.class);

    private ActorSystem system;
    private ActorRef sampleActor;
    private SampleActorStateTransitionGraph sampleActorStateTransitionGraph;

    @PostConstruct
    public void init() {
        Config config = ConfigFactory.load();
        system = ActorSystem.create("SampleActorSystem", config.getConfig("akka"));
        log.info("Actor system for sample started");

        initActors();
    }

    public ActorRef getSampleActor() {
        return sampleActor;
    }

    public SampleGuideBook createGuideBook() {
        return new SampleGuideBook(sampleActorStateTransitionGraph);
    }

    public void shutdown() {
        log.info("Shutting down sample actor system");
        system.shutdown();
        log.info("Completed shutdown of sample actor system");
    }

    private void initActors() {
        ActorRef dynamoLookupActor = //
                system.actorOf(Props.create(SampleDynamoLookupActor.class), //
                        "dynamoLookupActor");

        ActorRef dnbLookupActor = //
                system.actorOf(Props.create(SampleDynamoLookupActor.class), //
                        "dnbLookupActor");

        ActorRef dunsDomainBasedMicroEngineActor = //
                system.actorOf(Props.create(SampleDunsDomainBasedMicroEngineActor.class), //
                        "dunsDomainBasedMicroEngineActor");

        ActorRef domainBasedMicroEngineActor = //
                system.actorOf(Props.create(SampleDomainBasedMicroEngineActor.class), //
                        "domainBasedMicroEngineActor");

        ActorRef microEngine3Actor = //
                system.actorOf(Props.create(SampleDunsBasedMicroEngineActor.class), //
                        "dunsBasedMicroEngineActor");

        ActorRef microEngine4Actor = //
                system.actorOf(Props.create(SampleLocationBasedMicroEngineActor.class), //
                        "locationBasedMicroEngineActor");

        sampleActor = //
                system.actorOf(Props.create(SampleActor.class), //
                        "sampleActor");

        sampleActorStateTransitionGraph = new SampleActorStateTransitionGraph(sampleActor, //
                dunsDomainBasedMicroEngineActor, dynamoLookupActor, domainBasedMicroEngineActor, //
                microEngine3Actor, microEngine4Actor, dnbLookupActor);

        log.info("All sample actors started");
    }
}
