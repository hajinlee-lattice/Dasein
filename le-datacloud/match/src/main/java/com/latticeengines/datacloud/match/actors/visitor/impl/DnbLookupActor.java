package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchActorTemplate;

public class DnbLookupActor extends MatchActorTemplate {
    private static final Log log = LogFactory.getLog(DnbLookupActor.class);

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