package com.latticeengines.actors.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.domain.exposed.actors.VisitingHistory;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(VisitorActorTemplate.class);

    protected abstract GuideBook getGuideBook();

    /**
     * @param traveler:
     *            Message sent from actor within current decision graph
     *            Eg of actor type: MicroEngine, Junction
     * @return True: Pause/Stop travel for now. Do something else 
     *         False: Continue traveling immediately
     */
    protected abstract boolean process(Traveler traveler);

    /**
     * @param response:
     *            Message sent from external/assistant actor outside of current
     *            decision graph
     *            Eg of external actor: Anchor of other decision graph
     *            Eg of assistant actor: Lookup actor (match)
     */
    protected abstract void process(Response response);

    protected String getNextLocation(Traveler traveler) {
        return getGuideBook().next(ActorUtils.getPath(self()), traveler);
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof Traveler || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (isValidMessageType(msg)) {
            Traveler traveler = null;
            boolean rejected = false;
            if (msg instanceof Traveler) {
                traveler = (Traveler) msg;
                if (logCheckInNOut()) {
                    traveler.checkIn(getClass().getSimpleName());
                }
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received traveler " + traveler);
                }

                setOriginalSender(traveler, sender());
                boolean sentToExternalActor = process(traveler);
                if (sentToExternalActor) {
                    // unblock current actor
                    return;
                }
                rejected = true;
            } else if (msg instanceof Response) {
                Response response = (Response) msg;
                traveler = response.getTravelerContext();
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received a response for traveler " + traveler + ": " + response.getResult());
                }
                process(response);
                rejected = false;
            }

            if (traveler == null) {
                throw new NullPointerException("Traveler object should not be null at this step.");
            }

            travel(traveler, getSelf(), rejected);
        } else {
            unhandled(msg);
        }
    }

    @SuppressWarnings("deprecation")
    protected void travel(Traveler traveler, ActorRef currentActorRef, boolean rejected) {
        getGuideBook().logVisit(ActorUtils.getPath(currentActorRef), traveler);
        String nextLocation = getNextLocation(traveler);
        if (nextLocation == null) {
            nextLocation = traveler.getAnchorActorLocation();
        }
        ActorRef nextActorRef = getContext().actorFor(nextLocation);
        if (logCheckInNOut()) {
            VisitingHistory visitingHistory = traveler.checkOut(getClass().getSimpleName(), getActorName(nextActorRef));
            visitingHistory.setRejected(rejected);
            writeVisitingHistory(visitingHistory);
        }
        if (log.isDebugEnabled()) {
            log.debug(self() + " is sending traveler " + traveler + " to " + nextActorRef);
        }
        nextActorRef.tell(traveler, currentActorRef);
    }

    protected void writeVisitingHistory(VisitingHistory history) {
        // by default do nothing
    }

    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        if (traveler.getOriginalLocation() == null) {
            traveler.setOriginalLocation(ActorUtils.getPath(originalSender));
        }
    }

    protected String getActorName(ActorRef actorRef) {
        return ActorUtils.getPath(actorRef);
    }

    protected boolean logCheckInNOut() {
        return true;
    }

}
