package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("entityEmailBasedMicroEngineActor")
@Scope("prototype")
public class EntityEmailBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {

    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        // TODO To ignore AID matched by email only, after double think, it
        // needs more discussion
        // 1. There is no concept of email in Account match. Email is already
        // parsed to domain. Do we want to exclude email only, or exclude
        // domain only?
        // 2. Eg. Match input only has 2 fields: Email + Website. User provides
        // aa@yahoo.com & yahoo.com. Domain used by Account match is yahoo.com.
        // Should we reject it in this actor or not?
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        String aid = traveler.getEntityIds().get(BusinessEntity.Account.name());
        return (aid == null //
                || DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(aid)) //
                && tuple.getEmail() != null;
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
