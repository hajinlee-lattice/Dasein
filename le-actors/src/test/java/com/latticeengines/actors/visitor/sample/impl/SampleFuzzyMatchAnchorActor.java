package com.latticeengines.actors.visitor.sample.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchGuideBook;

import akka.actor.ActorRef;

@Component("sampleFuzzyMatchAnchorActor")
@Scope("prototype")
public class SampleFuzzyMatchAnchorActor extends VisitorActorTemplate {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SampleFuzzyMatchAnchorActor.class);

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
        traveler.setAnchorActorLocation(ActorUtils.getPath(self()));
        return false;
    }

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        if (traveler.getOriginalLocation() == null) {
            traveler.setOriginalLocation(ActorUtils.getPath(originalSender));
        }
    }
}
