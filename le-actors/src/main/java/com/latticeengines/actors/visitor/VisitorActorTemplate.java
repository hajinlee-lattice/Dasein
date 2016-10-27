package com.latticeengines.actors.visitor;

import org.apache.commons.logging.Log;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {

    protected abstract Log getLogger();

    protected abstract boolean isValidTravelerType(Traveler traveler);

    protected abstract Object process(Traveler traveler);

    protected abstract void travel(Traveler traveler, Object nextActorRef, Object currentActorRef);

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof Traveler && isValidTravelerType((Traveler) msg);
    }

    @Override
    protected void processMessage(Object travelerObj) {
        if (travelerObj instanceof Traveler //
                && isValidTravelerType((Traveler) travelerObj)) {
            Traveler traveler = (Traveler) travelerObj;
            getLogger().info("Received traveler: " + traveler);

            setOriginalSender(traveler, sender());

            Object result = process(traveler);

            traveler.setResult(result);

            GuideBook guideBook = traveler.getGuideBook();
            ActorRef nextActorRef = (ActorRef) guideBook //
                    .getDestination(getSelf(), traveler);

            getLogger().info("Send message to " + nextActorRef);

            travel(traveler, nextActorRef, getSelf());
        } else {
            unhandled(travelerObj);
        }
    }

    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        // do nothing
    }
}
