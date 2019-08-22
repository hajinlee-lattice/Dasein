package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.actors.visitor.AMLookupMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("domainCountryStateBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryStateBasedMicroEngineActor extends AMLookupMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(DomainCountryStateBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        return (matchKeyTuple.getDomain() != null && matchKeyTuple.getState() != null);
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
        traveler.addEntityLdcMatchTypeToTupleList(
                Pair.of(LdcMatchType.DOMAIN_COUNTRY_STATE, prepareInputData(traveler.getMatchKeyTuple())));
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s Country=%s State=%s )", keyTuple.getDomain(),
                keyTuple.getCountry() != null ? keyTuple.getCountry() : LocationUtils.USA, keyTuple.getState());
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
