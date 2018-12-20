package com.latticeengines.actors.template;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;

import akka.actor.ActorRef;

public abstract class AnchorActorTemplate extends VisitorActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(AnchorActorTemplate.class);

    protected abstract ActorSystemTemplate getActorSystem();

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
    protected void process(Response response) {
        // If anchor has assistant actor, process response from assistant actor
    }

    @Override
    protected String getActorName(ActorRef actorRef) {
        return getActorSystem().getActorName(actorRef);
    }
}
