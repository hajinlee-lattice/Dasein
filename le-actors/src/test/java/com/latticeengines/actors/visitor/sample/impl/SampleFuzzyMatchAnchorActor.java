package com.latticeengines.actors.visitor.sample.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchGuideBook;

import akka.actor.ActorRef;

@Component("sampleFuzzyMatchAnchorActor")
@Scope("prototype")
public class SampleFuzzyMatchAnchorActor extends VisitorActorTemplate {
    private static final Log log = LogFactory.getLog(SampleFuzzyMatchAnchorActor.class);

    @Autowired
    @Qualifier("sampleMatchGuideBook")
    protected SampleMatchGuideBook guideBook;

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof SampleMatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(Traveler traveler) {
        traveler.setAnchorActorLocation(self().path().toSerializationFormat());
        return false;
    }

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        if (traveler.getOriginalLocation() == null) {
            traveler.setOriginalLocation(originalSender.path().toSerializationFormat());
        }
    }

    // @Override
    // protected void handleResult(Traveler traveler, ActorRef originalSender) {
    // String originalLocation = traveler.getOriginalLocation();
    //
    // ActorRef nextActorRef = getContext().actorFor(originalLocation);
    //
    // log.debug("Send message to " + nextActorRef);
    //
    // sendResult(nextActorRef, traveler.getResult());
    // }
}
