package com.latticeengines.actors.template;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

import akka.actor.ActorRef;

/**
 * Actors in decision graph have 3 types: anchor, micro-engine & junction
 * 
 * Anchor is entry/exit actor
 * 
 * Micro-engine is actors where traveler travels around
 * 
 * Junction is the connecting point between decision graph/actor system
 */
public abstract class AnchorActorTemplate extends VisitorActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(AnchorActorTemplate.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean process(Traveler traveler) {
        // Inject failure only for testing purpose
        injectFailure(traveler);
        traveler.addRetry();
        if (shouldPrepareRetravel(traveler)) {
            traveler.prepareForRetravel();
            traveler.debug(String.format("Start traveling in decision graph %s for %d times",
                    traveler.getDecisionGraph(), traveler.getRetries()));
        }
        if (!traveler.isProcessed()) {
            // Just enter current decision graph or will re-travel in current
            // decision graph
            traveler.setAnchorActorLocation(ActorUtils.getPath(self()));
            return false;
        } else if (CollectionUtils.isNotEmpty(traveler.getTransitionHistory())) {
            // Finished traveling around current decision graph but need to
            // return to previous junction actor
            String junction = traveler.getTransitionHistory().peek().getJunction();
            ActorRef junctionRef = getActorSystem().getActorRef(junction);
            Response response = new Response();
            response.setTravelerContext(traveler);
            junctionRef.tell(response, self());
            return true;
        }
        // Completed whole trip, reduce retry count by one to compensate the
        // increment at the start of this function
        traveler.descRetry();
        return false;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        return true;
    }

    @Override
    protected boolean needAssistantActor() {
        return false;
    }

    @Override
    protected boolean skipIfRetravel(Traveler traveler) {
        return false;
    }

    protected boolean shouldPrepareRetravel(Traveler traveler) {
        if (traveler.getRetries() <= 1 || traveler.getResult() != null) {
            return false;
        }
        DecisionGraph decisionGraph;
        try {
            decisionGraph = getGuideBook().getDecisionGraphByName(traveler.getDecisionGraph());
        } catch (Exception e) {
            traveler.error("Failed to retrieve decision graph " + traveler.getDecisionGraph(), e);
            return false;
        }
        if (decisionGraph.getRetries() == null || traveler.getRetries() > decisionGraph.getRetries()) {
            return false;
        }
        return true;
    }

}
