package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorFactory;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationBasedMicroEngineActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {
    private static final Log log = LogFactory.getLog(MatchGuideBook.class);

    // this is sample graph, replace with actual logic
    private MatchActorStateTransitionGraph actorStateTransitionGraph;

    private ActorSystem system;

    private ActorRef fuzzyMatchAnchor;

    @Autowired
    private ActorFactory actorFactory;

    private HashMap<String, ActorRef> dataSourceActorRefMap;

    @PostConstruct
    public void init() {
        Config config = ConfigFactory.load();
        system = ActorSystem.create("FuzzyMatch", config.getConfig("akka"));
        log.info("Actor system for match started");

        dataSourceActorRefMap = new HashMap<String, ActorRef>();

        initActors();
    }

    @Override
    public String next(String currentLocation, TravelContext traveler) {
        String destinationLocation = getDestinationLocation(currentLocation, traveler);

        if (destinationLocation == null && currentLocation.equals(fuzzyMatchAnchor.path().toSerializationFormat())) {
            if (destinationLocation == null) {
                List<String> nextLocations = calculateNextVisitingActors(traveler);
                String[] nextLocationArray = new String[nextLocations.size()];
                int idx = 0;
                for (String location : nextLocations) {
                    nextLocationArray[idx++] = location;
                }
                traveler.setLocationInVisitingQueue(nextLocationArray);
                destinationLocation = getDestinationLocation(currentLocation, traveler);
            }
        }

        return destinationLocation;
    }

    public ActorRef getDataSourceActorRef(String dataSourceActor) {
        return dataSourceActorRefMap.get(dataSourceActor);
    }

    public ActorRef getFuzzyMatchAnchor() {
        return fuzzyMatchAnchor;
    }

    public void sendResponse(Object response, String returnAddress) {
        ActorRef ref = system.actorFor(returnAddress);
        ref.tell(response, null);
    }

    public void shutdown() {
        log.info("Shutting down match actor system");
        system.shutdown();
        log.info("Completed shutdown of match actor system");
    }

    private void initActors() {
        ActorRef dynamoLookupActor = //
                actorFactory.create(system, "dynamoLookupActor", //
                        DynamoLookupActor.class);
        dataSourceActorRefMap.put("dynamoLookupActor", dynamoLookupActor);
        ActorRef dnbLookupActor = //
                actorFactory.create(system, "dnbLookupActor", //
                        DnbLookupActor.class);
        dataSourceActorRefMap.put("dnbLookupActor", dnbLookupActor);

        ActorRef dunsDomainBasedMicroEngineActor = //
                actorFactory.create(system, "dunsDomainBasedMicroEngineActor", //
                        DunsDomainBasedMicroEngineActor.class);

        ActorRef domainBasedMicroEngineActor = //
                actorFactory.create(system, "domainBasedMicroEngineActor", //
                        DomainBasedMicroEngineActor.class);
        domainBasedMicroEngineActor.tell(dynamoLookupActor, null);

        ActorRef dunsBasedMicroEngineActor = //
                actorFactory.create(system, "dunsBasedMicroEngineActor", //
                        DunsBasedMicroEngineActor.class);
        dunsBasedMicroEngineActor.tell(dynamoLookupActor, null);

        ActorRef locationBasedMicroEngineActor = //
                actorFactory.create(system, "locationBasedMicroEngineActor", //
                        LocationBasedMicroEngineActor.class);
        locationBasedMicroEngineActor.tell(dnbLookupActor, null);

        fuzzyMatchAnchor = //
                actorFactory.create(system, "fuzzyMatchAnchorActor", //
                        FuzzyMatchAnchorActor.class);

        actorStateTransitionGraph = new MatchActorStateTransitionGraph(
                dunsDomainBasedMicroEngineActor.path().toSerializationFormat(),
                domainBasedMicroEngineActor.path().toSerializationFormat(),
                dunsBasedMicroEngineActor.path().toSerializationFormat(), //
                locationBasedMicroEngineActor.path().toSerializationFormat(),
                dunsDomainBasedMicroEngineActor.path().toSerializationFormat());
        log.info("All match actors started");
    }

    private List<String> calculateNextVisitingActors(TravelContext traveler) {
        String next = null;
        if (traveler.getVisitedHistory().size() == 0) {
            next = traveler.getGuideBook().next(null, traveler);
        } else {
            String latestMicroEngineLocation = traveler.getVisitedHistory()
                    .get(traveler.getVisitedHistory().size() - 1);
            next = traveler.getGuideBook().next(latestMicroEngineLocation, traveler);
        }
        List<String> visitingActors = new ArrayList<>();
        visitingActors.add(next);
        return visitingActors;
    }

    private String getDestinationLocation(String currentLocation, TravelContext traveler) {

        String destinationLocation = traveler.getNextLocationFromVisitingQueue();
        if (destinationLocation == null) {
            if (traveler.getResult() == null && currentLocation != null
                    && !currentLocation.equals(fuzzyMatchAnchor.path().toSerializationFormat())) {
                destinationLocation = actorStateTransitionGraph//
                        .next(currentLocation, traveler, traveler.getOriginalLocation());
            } else if (currentLocation == null) {
                destinationLocation = actorStateTransitionGraph.getDummyGraph().get(0);
            }

            if (destinationLocation == null) {
                return null;
            }
            traveler.setLocationInVisitingQueue(destinationLocation);
            destinationLocation = traveler.getNextLocationFromVisitingQueue();
        }

        log.info(destinationLocation);
        return destinationLocation;
    }

}
