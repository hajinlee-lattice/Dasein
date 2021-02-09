package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
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
        return !matchFoundInLookupMode(traveler) && StringUtils.isNotBlank(getDuns(traveler));
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        // only use duns from ldc to do lookup, don't use duns from customer
        // input unless it's same as ldc duns
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withDuns(getDuns(traveler))
                .build();
        return prepareLookupRequest(traveler, tuple);
    }

    private String getDuns(@NotNull MatchTraveler traveler) {
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        return traveler.getDunsOriginMap().get(DataCloudConstants.ACCOUNT_MASTER);
    }

}
