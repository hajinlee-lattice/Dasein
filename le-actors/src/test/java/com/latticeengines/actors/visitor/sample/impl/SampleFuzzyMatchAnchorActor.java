package com.latticeengines.actors.visitor.sample.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.VisitorActorTemplate;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchGuideBook;

import akka.actor.ActorRef;

@Component("sampleFuzzyMatchAnchorActor")
@Scope("prototype")
public class SampleFuzzyMatchAnchorActor extends VisitorActorTemplate {

    @Resource(name = "sampleMatchGuideBook")
    protected SampleMatchGuideBook guideBook;

    @Inject
    private SampleMatchActorSystem actorSystem;

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

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return actorSystem;
    }

    @Override
    protected boolean needAssistantActor() {
        return false;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        return true;
    }
}
