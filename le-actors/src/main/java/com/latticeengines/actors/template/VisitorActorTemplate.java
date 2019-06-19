package com.latticeengines.actors.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.actors.VisitingHistory;

import akka.actor.ActorRef;

public abstract class VisitorActorTemplate extends ActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(VisitorActorTemplate.class);

    protected abstract GuideBook getGuideBook();

    protected abstract ActorSystemTemplate getActorSystem();

    /**
     * Whether the actor need to call assistant actor to finish task
     * 
     * @return
     */
    protected abstract boolean needAssistantActor();

    /**
     * Whether the actor accept the traveler and take some actions
     * 
     * @param traveler
     * @return
     */
    protected abstract boolean accept(Traveler traveler);

    /**
     * @param traveler:
     *            Message sent from actor within current decision graph Eg of
     *            actor type: MicroEngine, Junction
     * @return True: Pause/Stop travel for now. Do something else False:
     *         Continue traveling immediately
     */
    protected abstract boolean process(Traveler traveler);

    /**
     * Actor which needs assistant actor to finish some task should override
     * this method
     * 
     * @param response:
     *            Message sent from external/assistant actor outside of current
     *            decision graph Eg of external actor: Anchor of other decision
     *            graph Eg of assistant actor: Lookup actor (match)
     */
    protected void process(Response response) {
        if (!needAssistantActor()) {
            log.error(String.format("Unexpected message recieved at %s: %s", ActorUtils.getPath(self()),
                    JsonUtils.serialize(response)));
            unhandled(response);
        }
    }

    /**
     * Based on current status/location of traveler and guide book, decide next
     * location to travel to
     * 
     * @param traveler
     * @return
     */
    protected String getNextLocation(Traveler traveler) {
        return getGuideBook().next(ActorUtils.getPath(self()), traveler);
    }

    /**
     * Return whether current actor should be skipped in retried travel
     * 
     * Some decision graph needs to retry the whole travel if target goal is not
     * met. But most of actors don't need to re-do the task for retried travel.
     * 
     * For actors which need to re-do the task in retried travel, override the
     * method
     * 
     * @return
     */
    protected boolean skipIfRetravel(Traveler traveler) {
        // Skip if it's retried travel
        return traveler.getRetries() > 1;
    }

    @Override
    protected void processMessage(Object msg) {
        if (isValidMessageType(msg)) {
            Traveler traveler = null;
            boolean rejected = false;
            if (msg instanceof Traveler) {
                traveler = (Traveler) msg;
                if (logCheckInNOut()) {
                    traveler.checkIn(getCurrentActorName());
                }
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received traveler " + traveler);
                }
                setOriginalSender(traveler, sender());

                try {
                    getGuideBook().logVisit(ActorUtils.getPath(self()), traveler);
                    if (skipIfRetravel(traveler)) {
                        traveler.debug("Skipped " + getActorSystem().getActorName(self()) + " in retried travel");
                    } else {
                        boolean sentToExternalActor = process(traveler);
                        if (sentToExternalActor) {
                            // unblock current actor
                            return;
                        }
                    }
                    rejected = true;
                } catch (Exception e) {
                    String errorMsg = String.format("Force to return anchor due to exception encountered at %s: %s",
                            getActorName(self()), e.getMessage());
                    traveler.error(errorMsg, e);
                    forceReturnToAnchor(traveler);
                    return;
                }
            } else if (msg instanceof Response) {
                Response response = (Response) msg;
                traveler = response.getTravelerContext();
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received a response for traveler " + traveler + ": " + response.getResult());
                }
                try {
                    process(response);
                    rejected = false;
                } catch (Exception e) {
                    traveler.error(String.format("Force to return anchor due to exception encountered at %s: %s",
                            getActorName(self()), e.getMessage()), e);
                    forceReturnToAnchor(traveler);
                    return;
                }
            }

            if (traveler == null) {
                throw new NullPointerException("Traveler object should not be null at this step.");
            }

            travel(traveler, self(), rejected);
        } else {
            unhandled(msg);
        }
    }

    @SuppressWarnings("deprecation")
    protected void travel(Traveler traveler, ActorRef currentActorRef, boolean rejected) {
        getGuideBook().logVisit(ActorUtils.getPath(currentActorRef), traveler);
        if (traveler.isReturnSender()) {
            ActorRef sender = getContext().actorFor(traveler.getOriginalLocation());
            sender.tell(traveler, self());
            return;
        }
        String nextLocation = getNextLocation(traveler);
        if (nextLocation == null) {
            nextLocation = traveler.getAnchorActorLocation();
        }
        ActorRef nextActorRef = getContext().actorFor(nextLocation);
        if (logCheckInNOut()) {
            VisitingHistory visitingHistory = traveler.checkOut(getCurrentActorName(), getActorName(nextActorRef));
            visitingHistory.setRejected(rejected);
            writeVisitingHistory(visitingHistory);
            handleVisits(traveler, visitingHistory);
        }
        if (log.isDebugEnabled()) {
            log.debug(self() + " is sending traveler " + traveler + " to " + nextActorRef);
        }
        nextActorRef.tell(traveler, currentActorRef);
    }

    /**
     * If unexpected/unhandled issue happens, force the traveler to return to
     * anchor to quit traveling, otherwise the traveler might get lost in the
     * actor system
     * 
     * @param traveler
     */
    @SuppressWarnings("deprecation")
    protected void forceReturnToAnchor(Traveler traveler) {
        // IsProcessed flag should already been set. Set again for safety
        traveler.setProcessed(true);
        ActorRef anchorRef = getContext().actorFor(traveler.getOriginalLocation());
        if (log.isDebugEnabled()) {
            log.debug(self() + " is sending traveler " + traveler + " to " + anchorRef);
        }
        anchorRef.tell(traveler, self());
    }

    protected void handleVisits(Traveler traveler, VisitingHistory history) {
        // by default do nothing
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
        return getActorSystem().getActorName(actorRef);
    }

    protected String getCurrentActorName() {
        return getActorName(self());
    }

    protected boolean logCheckInNOut() {
        return true;
    }

    // Only for testing purpose
    protected void injectFailure(Traveler traveler) {
        if (getCurrentActorName().equals(traveler.getActorOrServiceToInjectFailure())) {
            throw new RuntimeException(ActorUtils.INJECTED_FAILURE_MSG);
        }
    }

}
