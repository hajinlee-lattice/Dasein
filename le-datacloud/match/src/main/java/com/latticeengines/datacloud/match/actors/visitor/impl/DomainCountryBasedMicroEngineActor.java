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

@Component("domainCountryBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryBasedMicroEngineActor extends AMLookupMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(DomainCountryBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        if (matchKeyTuple.getState() != null || matchKeyTuple.getZipcode() != null) {
            //return (matchKeyTuple.getDomain() != null);

            // $JAW$
            if (matchKeyTuple.getDomain() != null) {
                traveler.addEntityLdcMatchTypeToTupleList(Pair.of(
                        EntityMatchType.LDC_DOMAIN_COUNTRY, prepareInputData(traveler.getMatchKeyTuple())));
                return true;
            } else {
                return false;
            }
        } else {
            //return (matchKeyTuple.getDomain() != null && matchKeyTuple.getCountry() != null);

            // $JAW$
            if (matchKeyTuple.getDomain() != null && matchKeyTuple.getCountry() != null) {
                traveler.addEntityLdcMatchTypeToTupleList(Pair.of(
                        EntityMatchType.LDC_DOMAIN_COUNTRY, prepareInputData(traveler.getMatchKeyTuple())));
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s Country=%s)", keyTuple.getDomain(),
                keyTuple.getCountry() != null ? keyTuple.getCountry() : LocationUtils.USA);
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple domainCountry = new MatchKeyTuple();
        domainCountry.setDomain(input.getDomain());
        domainCountry.setCountry(input.getCountry() != null ? input.getCountry() : LocationUtils.USA);
        return domainCountry;
    }
}
