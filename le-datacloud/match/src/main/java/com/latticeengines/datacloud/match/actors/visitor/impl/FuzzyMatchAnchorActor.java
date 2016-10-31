package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;

import akka.actor.ActorRef;

public class FuzzyMatchAnchorActor extends VisitorActorTemplate {
    private static final Log log = LogFactory.getLog(FuzzyMatchAnchorActor.class);
    private MatchGuideBook guideBook;

    public FuzzyMatchAnchorActor(MatchGuideBook guideBook) {
        super();
        this.guideBook = guideBook;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelContext traveler) {
        traveler.setAnchorActorLocation(self().path().toSerializationFormat());
        return false;
    }

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected void setOriginalSender(TravelContext traveler, ActorRef originalSender) {
        if (traveler.getOriginalLocation() == null) {
            traveler.setOriginalLocation(originalSender.path().toSerializationFormat());
        }
    }

    @Override
    protected void handleResult(Response response, TravelContext traveler) {
        String originalLocation = traveler.getOriginalLocation();

        ActorRef nextActorRef = getContext().actorFor(originalLocation);

        getLogger().info("Send message to " + nextActorRef);

        sendResult(nextActorRef, traveler.getResult());
    }

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }
}
