package com.latticeengines.actors.visitor;

import org.apache.commons.logging.Log;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {

    protected abstract Log getLogger();

    protected abstract boolean process(TravelContext traveler);

    protected abstract void process(Response response);

    protected abstract GuideBook getGuideBook();

    protected String getNextLocation(TravelContext traveler) {
        return getGuideBook().next(self().path().toSerializationFormat(), traveler);
        // return traveler.getNextLocationFromVisitingQueue();
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof TravelContext || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (isValidMessageType(msg)) {
            if (msg instanceof TravelContext) {
                TravelContext traveler = (TravelContext) msg;
                getLogger().info("Received traveler: " + traveler);

                setOriginalSender(traveler, sender());

                boolean hasSentMessageToDataSourceActor = process(traveler);

                if (!hasSentMessageToDataSourceActor) {

                    String nextLocation = getNextLocation(traveler);

                    if (nextLocation == null) {
                        nextLocation = traveler.getAnchorActorLocation();
                    }
                    ActorRef nextActorRef = getContext().actorFor(nextLocation);

                    getLogger().info("Send message to " + nextActorRef);

                    travel(traveler, nextActorRef, getSelf());
                }
            } else if (msg instanceof Response) {
                Response response = (Response) msg;

                process(response);

                TravelContext traveler = response.getTravelerContext();
                if (response.getResult() != null) {
                    traveler.setResult(response.getResult());
                }

                if (traveler.getResult() != null) {
                    handleResult(response, traveler);
                } else {
                    String anchor = traveler.getAnchorActorLocation();

                    ActorRef nextActorRef = getContext().actorFor(anchor);

                    getLogger().info("Send message to anchor " + nextActorRef);

                    sendResult(nextActorRef, traveler);
                }
            }
        } else {
            unhandled(msg);
        }
    }

    protected void handleResult(Response response, TravelContext traveler) {
        String anchorLocation = traveler.getAnchorActorLocation();

        ActorRef nextActorRef = getContext().actorFor(anchorLocation);

        getLogger().info("Send message to " + nextActorRef);

        sendResult(nextActorRef, response);// traveler.getResult());
    }

    protected void travel(TravelContext traveler, ActorRef nextActorRef, ActorRef currentActorRef) {
        getGuideBook().logVisit(((ActorRef) currentActorRef).path().toSerializationFormat(), traveler);
        nextActorRef.tell(traveler, currentActorRef);
    }

    protected void sendResult(ActorRef nextActorRef, Object result) {
        nextActorRef.tell(result, self());
    }

    protected void setOriginalSender(TravelContext traveler, ActorRef originalSender) {
        // do nothing
    }
}
