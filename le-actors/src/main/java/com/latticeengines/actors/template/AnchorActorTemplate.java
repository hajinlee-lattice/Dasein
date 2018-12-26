package com.latticeengines.actors.template;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;

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
        if (!traveler.isProcessed()) {
            // Just enter current decision graph
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
        // Completed whole trip
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

}
