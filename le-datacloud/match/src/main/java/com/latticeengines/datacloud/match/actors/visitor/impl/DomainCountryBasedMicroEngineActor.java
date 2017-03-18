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

@Component("domainCountryBasedMicroEngineActor")
@Scope("prototype")
public class DomainCountryBasedMicroEngineActor extends LookupMicroEngineActorTemplate {
    private static final Log log = LogFactory.getLog(DomainCountryBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();
        if (matchKeyTuple.getState() != null || matchKeyTuple.getZipcode() != null) {
            return (matchKeyTuple.getDomain() != null);
        } else {
            return (matchKeyTuple.getDomain() != null && matchKeyTuple.getCountry() != null);
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
