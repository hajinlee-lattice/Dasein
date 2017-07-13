package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.actors.visitor.LookupMicroEngineActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("domainCountryZipCodeBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryZipCodeBasedMicroEngineActor extends LookupMicroEngineActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(DomainCountryZipCodeBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();
        return (matchKeyTuple.getDomain() != null && matchKeyTuple.getZipcode() != null);
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s Country=%s ZipCode=%s )", keyTuple.getDomain(),
                keyTuple.getCountry() != null ? keyTuple.getCountry() : LocationUtils.USA,
                keyTuple.getZipcode());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple domainCountryZipCode = new MatchKeyTuple();
        domainCountryZipCode.setDomain(input.getDomain());
        domainCountryZipCode.setCountry(input.getCountry() != null ? input.getCountry() : LocationUtils.USA);
        domainCountryZipCode.setZipcode(input.getZipcode());
        return domainCountryZipCode;
    }
}
