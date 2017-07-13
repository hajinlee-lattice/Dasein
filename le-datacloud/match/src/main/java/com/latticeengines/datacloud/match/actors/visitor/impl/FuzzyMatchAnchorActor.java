package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

import akka.actor.ActorRef;

@Component("fuzzyMatchAnchorActor")
@Scope("prototype")
public class FuzzyMatchAnchorActor extends VisitorActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(FuzzyMatchAnchorActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler || msg instanceof Response;
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

    @Override
    protected String getActorName(ActorRef actorRef) {
        return matchActorSystem.getActorName(actorRef);
    }

    @Override
    protected boolean logCheckInNOut() {
        return false;
    }

}
