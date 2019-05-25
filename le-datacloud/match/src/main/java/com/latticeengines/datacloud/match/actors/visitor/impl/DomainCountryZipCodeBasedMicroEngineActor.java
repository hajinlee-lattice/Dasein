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
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("domainCountryZipCodeBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryZipCodeBasedMicroEngineActor extends AMLookupMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(DomainCountryZipCodeBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        if (matchKeyTuple.getDomain() != null && matchKeyTuple.getZipcode() != null) {
            traveler.addEntityLdcMatchTypeToTupleList(Pair.of(
                    EntityMatchType.LDC_DOMAIN_COUNTRY_ZIPCODE, prepareInputData(traveler.getMatchKeyTuple())));
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s Country=%s ZipCode=%s )", keyTuple.getDomain(),
                keyTuple.getCountry() != null ? keyTuple.getCountry() : LocationUtils.USA, keyTuple.getZipcode());
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
