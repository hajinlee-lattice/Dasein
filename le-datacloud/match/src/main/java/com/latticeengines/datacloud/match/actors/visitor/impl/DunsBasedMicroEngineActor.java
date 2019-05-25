package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.AMLookupMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dunsBasedMicroEngineActor")
@Scope("prototype")
public class DunsBasedMicroEngineActor extends AMLookupMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(DunsBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        if (matchKeyTuple.getDuns() != null) {
            traveler.addEntityLdcMatchTypeToTupleList(Pair.of(
                    EntityMatchType.LDC_DUNS, prepareInputData(traveler.getMatchKeyTuple())));
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( DUNS=%s )", keyTuple.getDuns());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple dunsOnly = new MatchKeyTuple();
        dunsOnly.setDuns(input.getDuns());
        return dunsOnly;
    }

}
