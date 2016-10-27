package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchActorTemplate;

import akka.actor.ActorRef;

public class MatchActor extends MatchActorTemplate {
    private static final Log log = LogFactory.getLog(MatchActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected Object process(Traveler traveler) {
        // MatchActor does not need to do anything, it just need to forward
        // message
        return null;
    }

    @Override
    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        if (traveler.getOriginalSender() == null) {
            log.info("MatchTraveler detected for the first time, setting original sender: " + originalSender);
            traveler.setOriginalSender(originalSender);
        }
    }
}
