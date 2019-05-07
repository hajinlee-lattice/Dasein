package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/**
 * For Contact lookup if Email is provided, but matched AccountId is anonymous
 * or Account match key is Email only
 */
@Component("entityEmailBasedMicroEngineActor")
@Scope("prototype")
public class EntityEmailBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {

    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        return EntityMatchUtils.hasEmailAccountInfoOnly(traveler);
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        MatchKeyTuple lookupTuple = new MatchKeyTuple.Builder() //
                .withEmail(tuple.getEmail()) //
                .build();
        return prepareLookupRequest(traveler, lookupTuple);
    }
}
