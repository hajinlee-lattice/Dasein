package com.latticeengines.actors.template;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AnchorActorTemplate extends VisitorActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(AnchorActorTemplate.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }
}
