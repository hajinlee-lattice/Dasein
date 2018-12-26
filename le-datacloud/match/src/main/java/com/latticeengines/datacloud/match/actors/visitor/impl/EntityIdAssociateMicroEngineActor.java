package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

@Component("entityIdAssociateMicroEngineActor")
@Scope("prototype")
public class EntityIdAssociateMicroEngineActor extends DataSourceMicroEngineTemplate<EntityAssociateActor> {

    @Override
    protected Class<EntityAssociateActor> getDataSourceActorClz() {
        return EntityAssociateActor.class;
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        return false;
    }

    @Override
    protected void process(Response response) {
    }
}
