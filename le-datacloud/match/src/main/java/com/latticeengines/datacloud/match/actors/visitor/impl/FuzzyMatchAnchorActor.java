package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
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
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        if (!matchTraveler.isProcessed()) {
            // Just enter current decision graph
            traveler.setAnchorActorLocation(ActorUtils.getPath(self()));
            return false;
        } else if (CollectionUtils.isNotEmpty(matchTraveler.getTransitionHistory())) {
            // Finished traveling around current decision graph but need to
            // return to previous junction actor
            String junction = matchTraveler.getTransitionHistory().peek().getJunction();
            ActorRef junctionRef = matchActorSystem.getActorRef(junction);
            Response response = new Response();
            response.setTravelerContext(matchTraveler);
            junctionRef.tell(response, self());
            return true;
        }
        // Completed whole trip
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
    protected String getActorName(ActorRef actorRef) {
        return matchActorSystem.getActorName(actorRef);
    }

    @Override
    protected boolean logCheckInNOut() {
        return false;
    }

}
