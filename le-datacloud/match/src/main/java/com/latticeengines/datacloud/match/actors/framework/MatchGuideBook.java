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
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
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
        dummyPathGraph
                .add(actorSystem.getActorRef(DunsDomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(actorSystem.getActorRef(DunsBasedMicroEngineActor.class).path().toSerializationFormat());
    }

    @Override
    public String next(String currentLocation, Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        if (fuzzyMatchAnchor.path().toSerializationFormat().equals(currentLocation)) {
            return nextMoveForAnchor(matchTraveler);
        } else {
            return nextMoveForMicroEngine(matchTraveler);
        }
    }

    @Override
    public void logVisit(String traversedActor, Traveler traveler) {
        traveler.logVisitHistory(traversedActor);
        if (traveler.visitingQueueIsEmpty()) {
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
        }
    }

    private String nextMoveForAnchor(MatchTraveler traveler) {
        if (!traveler.isProcessed()) {
            traveler.setProcessed(true);
            // initialization
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
            return traveler.getNextLocationFromVisitingQueue();
        } else {
            return traveler.getOriginalLocation();
        }
    }

    private String nextMoveForMicroEngine(MatchTraveler traveler) {
        String destinationLocation;

        if (!traveler.isMatched()) {
            do {
                destinationLocation = traveler.getNextLocationFromVisitingQueue();
                if (!visitSameMicroEngineWithSameDataAgain(destinationLocation, traveler)) {
                    return destinationLocation;
                }
            } while (StringUtils.isNotEmpty(destinationLocation));
        }

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination, MatchTraveler traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        String currentContext = JsonUtils.serialize(traveler.getMatchKeyTuple());
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            return previousData.contains(currentContext);
        } else {
            return false;
        }
    }

}
