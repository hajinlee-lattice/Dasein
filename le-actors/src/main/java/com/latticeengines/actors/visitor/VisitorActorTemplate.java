package com.latticeengines.actors.visitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {
    private static final Log log = LogFactory.getLog(VisitorActorTemplate.class);

    @Autowired
    @Qualifier("matchGuideBook")
    protected GuideBook guideBook;

    protected abstract boolean process(TravelContext traveler);

    protected abstract void process(Response response);

    protected String getNextLocation(TravelContext traveler) {
        return guideBook.next(self().path().toSerializationFormat(), traveler);
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
                log.debug("Received traveler: " + traveler);

                setOriginalSender(traveler, sender());

                boolean hasSentMessageToDataSourceActor = process(traveler);

                if (!hasSentMessageToDataSourceActor) {

                    String nextLocation = getNextLocation(traveler);

                    if (nextLocation == null) {
                        nextLocation = traveler.getAnchorActorLocation();
                    }
                    ActorRef nextActorRef = getContext().actorFor(nextLocation);

                    log.debug("Send message to " + nextActorRef);

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
                    String nextLocation = getNextLocation(traveler);

                    if (nextLocation == null || response.getResult() != null) {
                        nextLocation = traveler.getAnchorActorLocation();
                    }
                    ActorRef nextActorRef = getContext().actorFor(nextLocation);
                    
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

        log.info("Send message to " + nextActorRef);

        sendResult(nextActorRef, response);
    }

    protected void travel(TravelContext traveler, ActorRef nextActorRef, ActorRef currentActorRef) {
        guideBook.logVisit(((ActorRef) currentActorRef).path().toSerializationFormat(), traveler);
        nextActorRef.tell(traveler, currentActorRef);
    }

    protected void sendResult(ActorRef nextActorRef, Object result) {
        nextActorRef.tell(result, self());
    }

    protected void setOriginalSender(TravelContext traveler, ActorRef originalSender) {
        // do nothing
    }
}
