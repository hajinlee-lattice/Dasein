package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("entitySystemIdBasedMicroEngineActor")
@Scope("prototype")
public class EntitySystemIdBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {

    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        return !matchFoundInLookupMode(traveler) && CollectionUtils.isNotEmpty(tuple.getSystemIds());
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        // only use system ids for lookup
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withSystemIds(traveler.getMatchKeyTuple().getSystemIds())
                .build();
        return prepareLookupRequest(traveler, tuple);
    }
}
