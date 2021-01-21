package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * For Contact lookup if Email is provided, matched AccountId is not anonymous
 * and Account match key is not Email only
 */
@Component("entityEmailAIDBasedMicroEngineActor")
@Scope("prototype")
public class EntityEmailAIDBasedMicroEngineActor extends EntityMicroEngineActorBase<EntityLookupActor> {

    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        MatchKeyTuple accountTuple = traveler.getEntityMatchKeyTuple(BusinessEntity.Account.name());
        String aid = traveler.getEntityIds().get(BusinessEntity.Account.name());
        // Assumption is: If Contact has Email, it must be mapped in
        // Account Domain match key because in Account match, there is
        // no concept of "Email", thus we can only detect how many
        // domain fields are mapped
        return !matchFoundInLookupMode(traveler) && !(aid == null || DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(aid)) //
                && tuple.getEmail() != null //
                && !(accountTuple != null && accountTuple.hasDomainOnly()
                        && !accountTuple.isDomainFromMultiCandidates());
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        String aid = traveler.getEntityIds().get(BusinessEntity.Account.name());
        MatchKeyTuple lookupTuple = new MatchKeyTuple.Builder() //
                .withEmail(tuple.getEmail()) //
                .withSystemIds(Arrays.asList(Pair.of(InterfaceName.AccountId.name(), aid))) //
                .build();
        return prepareLookupRequest(traveler, lookupTuple);
    }
}
