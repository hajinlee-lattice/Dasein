package com.latticeengines.datacloud.match.actors.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;

import akka.actor.ActorRef;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {

    private static final Log log = LogFactory.getLog(MatchGuideBook.class);

    private ActorRef fuzzyMatchAnchor;

    @Autowired
    private MatchActorSystem actorSystem;

    private List<String> dummyPathGraph;

    @PostConstruct
    public void init() {
        log.info("Initialize fuzzy match guide book.");
        fuzzyMatchAnchor = actorSystem.getFuzzyMatchAnchor();

        dummyPathGraph = new ArrayList<>();
        dummyPathGraph
                .add(actorSystem.getActorRef(DunsDomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DunsBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph
                .add(actorSystem.getActorRef(LocationToDunsMicroEngineActor.class).path().toSerializationFormat());
    }

    @Override
    public String next(String currentLocation, TravelContext traveler) {
        MatchTravelContext matchTravelContext = (MatchTravelContext) traveler;
        if (fuzzyMatchAnchor.path().toSerializationFormat().equals(currentLocation)) {
            return nextMoveForAnchor(matchTravelContext);
        } else {
            return nextMoveForMicroEngine(matchTravelContext);
        }
    }

    @Override
    public void logVisit(String traversedActor, TravelContext traveler) {
        traveler.logVisitHistory(traversedActor);
        if (traveler.visitingQueueIsEmpty()) {
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
        }
    }

    private String nextMoveForAnchor(MatchTravelContext traveler) {
        if (!traveler.isProcessed()) {
            traveler.setProcessed(true);
            // initialization
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
            return traveler.getNextLocationFromVisitingQueue();
        } else {
            return traveler.getOriginalLocation();
        }
    }

    private String nextMoveForMicroEngine(MatchTravelContext traveler) {
        String destinationLocation;

        do {
            destinationLocation = traveler.getNextLocationFromVisitingQueue();
            if (!visitSameMicroEngineWithSameDataAgain(destinationLocation, traveler)) {
                return destinationLocation;
            }
        } while (StringUtils.isNotEmpty(destinationLocation));

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination, MatchTravelContext traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            return previousData.contains(JsonUtils.serialize(traveler.getMatchKeyTuple()));
        } else {
            return false;
        }
    }

}
