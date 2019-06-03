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

@Component("dunsDomainBasedMicroEngineActor")
@Scope("prototype")
public class DunsDomainBasedMicroEngineActor extends AMLookupMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(DunsDomainBasedMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<DynamoLookupActor> getDataSourceActorClz() {
        return DynamoLookupActor.class;
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        return (matchKeyTuple.getDomain() != null && matchKeyTuple.getDuns() != null);
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
        traveler.addEntityLdcMatchTypeToTupleList(
                Pair.of(EntityMatchType.LDC_DUNS_DOMAIN, prepareInputData(traveler.getMatchKeyTuple())));
    }

    @Override
    protected String usedKeys(MatchKeyTuple keyTuple) {
        return String.format("( Domain=%s, DUNS=%s )", keyTuple.getDomain(), keyTuple.getDuns());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple domainDuns = new MatchKeyTuple();
        domainDuns.setDuns(input.getDuns());
        domainDuns.setDomain(input.getDomain());

        return domainDuns;
    }
}
