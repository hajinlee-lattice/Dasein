package com.latticeengines.actors.visitor.sample.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.impl.SampleDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleDunsDomainBasedMicroEngineActor;
import com.latticeengines.actors.visitor.sample.impl.SampleLocationToDunsMicroEngineActor;
import com.latticeengines.common.exposed.util.JsonUtils;

import akka.actor.ActorRef;

@Component("sampleMatchGuideBook")
public class SampleMatchGuideBook extends GuideBook {

    private static final Log log = LogFactory.getLog(SampleMatchGuideBook.class);

    private ActorRef fuzzyMatchAnchor;

    @Autowired
    private SampleMatchActorSystem actorSystem;

    private List<String> dummyPathGraph;

    @PostConstruct
    public void init() {
        log.info("Initialize fuzzy match guide book.");
        fuzzyMatchAnchor = actorSystem.getFuzzyMatchAnchor();

        dummyPathGraph = new ArrayList<>();
        dummyPathGraph.add(
                actorSystem.getActorRef(SampleDunsDomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph
                .add(actorSystem.getActorRef(SampleDomainBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph
                .add(actorSystem.getActorRef(SampleDunsBasedMicroEngineActor.class).path().toSerializationFormat());
        dummyPathGraph.add(
                actorSystem.getActorRef(SampleLocationToDunsMicroEngineActor.class).path().toSerializationFormat());
    }

    @Override
    public String next(String currentLocation, Traveler traveler) {
        SampleMatchTravelContext matchTravelContext = (SampleMatchTravelContext) traveler;
        if (fuzzyMatchAnchor.path().toSerializationFormat().equals(currentLocation)) {
            return nextMoveForAnchor(matchTravelContext);
        } else {
            return nextMoveForMicroEngine(matchTravelContext);
        }
    }

    @Override
    public void logVisit(String traversedActor, Traveler traveler) {
        traveler.logVisitHistory(traversedActor);
        if (traveler.visitingQueueIsEmpty()) {
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
        }
    }

    private String nextMoveForAnchor(SampleMatchTravelContext traveler) {
        if (!traveler.isProcessed()) {
            traveler.setProcessed(true);
            // initialization
            traveler.addLocationsToVisitingQueue(dummyPathGraph.toArray(new String[dummyPathGraph.size()]));
            return traveler.getNextLocationFromVisitingQueue();
        } else {
            return traveler.getOriginalLocation();
        }
    }

    private String nextMoveForMicroEngine(SampleMatchTravelContext traveler) {
        String destinationLocation;

        do {
            destinationLocation = traveler.getNextLocationFromVisitingQueue();
            if (!visitSameMicroEngineWithSameDataAgain(destinationLocation, traveler)) {
                return destinationLocation;
            }
        } while (StringUtils.isNotEmpty(destinationLocation));

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination,
            SampleMatchTravelContext traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            return previousData.contains(JsonUtils.serialize(traveler.getMatchKeyTuple()));
        } else {
            return false;
        }
    }

}
