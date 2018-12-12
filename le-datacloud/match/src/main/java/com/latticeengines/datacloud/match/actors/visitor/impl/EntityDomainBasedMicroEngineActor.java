package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

@Component("entityDomainBasedMicroEngineActor")
@Scope("prototype")
public class EntityDomainBasedMicroEngineActor extends MicroEngineActorTemplate<EntityLookupActor> {
    @Override
    protected Class<EntityLookupActor> getDataSourceActorClz() {
        return EntityLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        return false;
    }

    @Override
    protected void process(Response response) {
    }
}