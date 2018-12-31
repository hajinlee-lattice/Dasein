package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("entityNameCountryBasedMicroEngineActor")
@Scope("prototype")
public class EntityNameCountryBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {

    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        return StringUtils.isNotBlank(tuple.getName()) && StringUtils.isNotBlank(tuple.getCountry());
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        // only use system ids for lookup
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withName(traveler.getMatchKeyTuple().getName())
                .withCountry(traveler.getMatchKeyTuple().getCountry())
                .build();
        return prepareLookupRequest(traveler, tuple);
    }
}
