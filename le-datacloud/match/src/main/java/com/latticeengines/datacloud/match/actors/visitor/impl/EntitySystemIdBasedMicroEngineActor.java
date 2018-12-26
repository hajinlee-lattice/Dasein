package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

@Component("entitySystemIdBasedMicroEngineActor")
@Scope("prototype")
public class EntitySystemIdBasedMicroEngineActor extends DataSourceMicroEngineTemplate<EntityLookupActor> {
    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        return false;
    }

    @Override
    protected void process(Response response) {
    }
}
