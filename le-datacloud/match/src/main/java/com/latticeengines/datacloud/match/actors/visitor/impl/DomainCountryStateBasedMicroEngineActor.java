package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.actors.visitor.LookupMicroEngineActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("domainCountryStateBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryStateBasedMicroEngineActor extends LookupMicroEngineActorTemplate {
    private static final Log log = LogFactory.getLog(DomainCountryStateBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();
        return (matchKeyTuple.getDomain() != null && matchKeyTuple.getState() != null);
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s Country=%s State=%s )", keyTuple.getDomain(),
                keyTuple.getCountry() != null ? keyTuple.getCountry() : LocationUtils.USA,
                keyTuple.getState());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple domainCountryState = new MatchKeyTuple();
        domainCountryState.setDomain(input.getDomain());
        domainCountryState.setCountry(input.getCountry() != null ? input.getCountry() : LocationUtils.USA);
        domainCountryState.setState(input.getState());
        return domainCountryState;
    }
}
