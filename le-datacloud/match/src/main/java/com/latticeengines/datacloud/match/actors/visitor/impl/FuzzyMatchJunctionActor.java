package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.JunctionActorTemplate;

/**
 * Junction actor to jump to LDC fuzzy match
 *
 */
@Component("fuzzyMatchJunctionActor")
@Scope("prototype")
public class FuzzyMatchJunctionActor extends JunctionActorTemplate {

    @Override
    protected boolean accept(Traveler traveler) {
        // TODO: to implement
        return true;
    }

}
