package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

import javax.inject.Inject;

@Component("entityIdAssociateMicroEngineActor")
@Scope("prototype")
public class EntityIdAssociateMicroEngineActor extends EntityMicroEngineActorBase<EntityAssociateActor> {

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Override
    protected Class<EntityAssociateActor> getDataSourceActorClz() {
        return EntityAssociateActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        // only accept if the system is in allocate mode
        return entityMatchConfigurationService.isAllocateMode();
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
