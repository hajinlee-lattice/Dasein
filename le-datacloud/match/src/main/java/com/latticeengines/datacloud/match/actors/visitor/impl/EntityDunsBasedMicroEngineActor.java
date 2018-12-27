package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("entityDunsBasedMicroEngineActor")
@Scope("prototype")
public class EntityDunsBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {
    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        return StringUtils.isNotBlank(tuple.getDuns());
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        // only use system ids for lookup
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withDuns(traveler.getMatchKeyTuple().getDuns())
                .build();
        return prepareLookupRequest(traveler, tuple);
    }
}
