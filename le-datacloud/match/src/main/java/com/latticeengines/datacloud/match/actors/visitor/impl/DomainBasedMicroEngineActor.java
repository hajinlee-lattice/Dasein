package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.LookupMicroEngineActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("domainBasedMicroEngineActor")
@Scope("prototype")
public class DomainBasedMicroEngineActor extends LookupMicroEngineActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(DomainBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();
        return matchKeyTuple.getDomain() != null;
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s )", keyTuple.getDomain());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple domainOnly = new MatchKeyTuple();
        domainOnly.setDomain(input.getDomain());
        return domainOnly;
    }

}
