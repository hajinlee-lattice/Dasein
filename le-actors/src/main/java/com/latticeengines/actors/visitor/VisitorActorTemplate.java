package com.latticeengines.actors.visitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {
    private static final Log log = LogFactory.getLog(VisitorActorTemplate.class);

    protected abstract GuideBook getGuideBook();

    protected abstract boolean process(Traveler traveler);

    protected abstract void process(Response response);

    protected String getNextLocation(Traveler traveler) {
        return getGuideBook().next(self().path().toSerializationFormat(), traveler);
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof Traveler || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (isValidMessageType(msg)) {
            Traveler traveler = null;
            if (msg instanceof Traveler) {
                traveler = (Traveler) msg;
                log.debug(self() + " received " + traveler);

                setOriginalSender(traveler, sender());
                boolean hasSentMessageToDataSourceActor = process(traveler);
                if (hasSentMessageToDataSourceActor) {
                    // unblock current actor
                    return;
                }

            } else if (msg instanceof Response) {
                Response response = (Response) msg;
                process(response);
                traveler = response.getTravelerContext();
                log.debug(self() + " received a response for " + traveler);
            }

            if (traveler == null) {
                throw new NullPointerException("Traveler object should not be null at this step.");
            }

            travel(traveler, getSelf());
        } else {
            unhandled(msg);
        }
    }

    protected void travel(Traveler traveler, ActorRef currentActorRef) {
        String nextLocation = getNextLocation(traveler);
        if (nextLocation == null) {
            nextLocation = traveler.getAnchorActorLocation();
        }
        ActorRef nextActorRef = getContext().actorFor(nextLocation);
        log.debug(self() + " sent " + traveler + " to " + nextActorRef);

        getGuideBook().logVisit(currentActorRef.path().toSerializationFormat(), traveler);
        nextActorRef.tell(traveler, currentActorRef);
    }

    protected void sendResult(ActorRef nextActorRef, Object result) {
        nextActorRef.tell(result, self());
    }

    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        // do nothing
    }
}
