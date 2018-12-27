package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

@Component("entityIdAssociateMicroEngineActor")
@Scope("prototype")
public class EntityIdAssociateMicroEngineActor extends EntityMicroEngineActorBase<EntityAssociateActor> {

    @Override
    protected Class<EntityAssociateActor> getDataSourceActorClz() {
        return EntityAssociateActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        // accept every tuple since we need to handle association even if there is no lookup entries (anonymous entity)
        return true;
    }

    @Override
    protected void process(Response response) {
        handleAssociationResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        return prepareAssociationRequest(traveler);
    }
}
