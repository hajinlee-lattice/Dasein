package com.latticeengines.actors.visitor.sample.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.sample.SampleActorTemplate;

import akka.actor.ActorRef;

public class SampleActor extends SampleActorTemplate {
    private static final Log log = LogFactory.getLog(SampleActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected Object process(Traveler traveler) {
        // SampleActor does not need to do anything, it just need to forward
        // message
        return null;
    }

    @Override
    protected void setOriginalSender(Traveler traveler, ActorRef originalSender) {
        if (traveler.getOriginalSender() == null) {
            log.info("SampleTraveler detected for the first time, setting original sender: " + originalSender);
            traveler.setOriginalSender(originalSender);
        }
    }
}
