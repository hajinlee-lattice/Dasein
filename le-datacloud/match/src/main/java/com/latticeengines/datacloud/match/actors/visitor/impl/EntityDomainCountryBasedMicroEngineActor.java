package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("entityDomainCountryBasedMicroEngineActor")
@Scope("prototype")
public class EntityDomainCountryBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {
    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        return !matchFoundInLookupMode(traveler) && StringUtils.isNotBlank(traveler.getMatchKeyTuple().getDomain());
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withDomain(traveler.getMatchKeyTuple().getDomain())
                .withCountry(StringUtils.isNotBlank(traveler.getMatchKeyTuple().getCountry()) ?
                        traveler.getMatchKeyTuple().getCountry() : LocationUtils.USA)
                .build();
        return prepareLookupRequest(traveler, tuple);
    }
}
