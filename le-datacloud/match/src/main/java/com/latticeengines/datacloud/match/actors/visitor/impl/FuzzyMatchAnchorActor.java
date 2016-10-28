package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelerContext;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelerContext;

import akka.actor.ActorRef;

public class FuzzyMatchAnchorActor extends VisitorActorTemplate {
    private static final Log log = LogFactory.getLog(FuzzyMatchAnchorActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelerContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelerContext traveler) {
        traveler.setAnchorActorLocation(self().path().toSerializationFormat());
        List<String> nextLocations = calculateNextVisitingActors(traveler);
        String[] nextLocationArray = new String[nextLocations.size()];
        int idx = 0;
        for (String location : nextLocations) {
            nextLocationArray[idx++] = location;
        }
        traveler.setLocationInVisitingQueue(nextLocationArray);
        return false;
    }

    private List<String> calculateNextVisitingActors(TravelerContext traveler) {
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

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected void setOriginalSender(TravelerContext traveler, ActorRef originalSender) {
        if (traveler.getOriginalLocation() == null) {
            traveler.setOriginalLocation(originalSender.path().toSerializationFormat());
        }
    }

    @Override
    protected void handleResult(Response response, TravelerContext traveler) {
        String originalLocation = traveler.getOriginalLocation();

        ActorRef nextActorRef = getContext().actorFor(originalLocation);

        getLogger().info("Send message to " + nextActorRef);

        sendResult(nextActorRef, traveler.getResult());
    }
}
