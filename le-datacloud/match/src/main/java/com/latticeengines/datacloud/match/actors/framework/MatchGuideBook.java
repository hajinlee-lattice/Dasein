package com.latticeengines.datacloud.match.actors.framework;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;

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
        dummyPathGraph.add(actorSystem.getActorRef(DunsDomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DunsBasedMicroEngineActor.class).path().toSerializationFormat());
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
        String destinationLocation = traveler.getNextLocationFromVisitingQueue();
        if (StringUtils.isEmpty(destinationLocation)) {
            return traveler.getAnchorActorLocation();
        } else {
            return destinationLocation;
        }
    }

    private List<String> calculateNextVisitingActors(TravelContext traveler) {
        String next = null;
        if (traveler.getVisitedHistory().size() == 0) {
            next = next(null, traveler);
        } else {
            String latestMicroEngineLocation = traveler.getVisitedHistory()
                    .get(traveler.getVisitedHistory().size() - 1);
            next = next(latestMicroEngineLocation, traveler);
        }
        List<String> visitingActors = new ArrayList<>();
        visitingActors.add(next);
        return visitingActors;
    }

}
