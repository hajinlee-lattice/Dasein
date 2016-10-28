package com.latticeengines.actors.visitor;

import org.apache.commons.logging.Log;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelerContext;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {

    protected abstract Log getLogger();

    protected abstract boolean process(TravelerContext traveler);

    protected abstract void process(Response response);

    protected String getNextLocation(TravelerContext traveler) {
        return traveler.getNextLocationFromVisitingQueue();
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof TravelerContext || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (isValidMessageType(msg)) {
            if (msg instanceof TravelerContext) {
                TravelerContext traveler = (TravelerContext) msg;
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

                TravelerContext traveler = response.getTravelerContext();
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

    protected void handleResult(Response response, TravelerContext traveler) {
        String anchorLocation = traveler.getAnchorActorLocation();

        ActorRef nextActorRef = getContext().actorFor(anchorLocation);

        getLogger().info("Send message to " + nextActorRef);

        sendResult(nextActorRef, response);// traveler.getResult());
    }

    protected void travel(TravelerContext traveler, ActorRef nextActorRef, ActorRef currentActorRef) {
        traveler.updateVisitedHistoryInfo(((ActorRef) currentActorRef).path().toSerializationFormat());
        nextActorRef.tell(traveler, currentActorRef);
    }

    protected void sendResult(ActorRef nextActorRef, Object result) {
        nextActorRef.tell(result, self());
    }

    protected void setOriginalSender(TravelerContext traveler, ActorRef originalSender) {
        // do nothing
    }
}
