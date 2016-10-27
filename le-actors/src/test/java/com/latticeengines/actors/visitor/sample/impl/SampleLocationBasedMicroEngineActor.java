package com.latticeengines.actors.visitor.sample.impl;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.sample.SampleActorTemplate;

public class SampleLocationBasedMicroEngineActor extends SampleActorTemplate {
    private static final Log log = LogFactory.getLog(SampleLocationBasedMicroEngineActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected Object process(Traveler traveler) {
        log.info("Try processing message");

        if (RandomUtils.nextLong(0, 4) == 0) {
            log.info("Found result");
            return UUID.randomUUID().toString();
        }
        return null;
    }
}