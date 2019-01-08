package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DnBMatchUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
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
        return StringUtils.isNotBlank(getDuns(traveler));
    }

    @Override
    protected void process(Response response) {
        handleLookupResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        // only use system ids for lookup
        MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                .withDuns(getDuns(traveler))
                .build();
        return prepareLookupRequest(traveler, tuple);
    }

    private String getDuns(@NotNull MatchTraveler traveler) {
        if (StringUtils.isNotBlank(traveler.getMatchKeyTuple().getDuns())) {
            // use user specified DUNS
            return traveler.getMatchKeyTuple().getDuns();
        }

        // choose DUNS from DnB
        return DnBMatchUtils.getFinalParentDuns(getFirstContext(traveler));
    }

    private DnBMatchContext getFirstContext(@NotNull MatchTraveler traveler) {
        if (CollectionUtils.isEmpty(traveler.getDnBMatchContexts())) {
            return null;
        }
        return traveler.getDnBMatchContexts().get(0);
    }
}
