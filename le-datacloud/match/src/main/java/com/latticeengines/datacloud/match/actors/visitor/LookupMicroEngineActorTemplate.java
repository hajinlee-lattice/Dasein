package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;

public abstract class LookupMicroEngineActorTemplate extends MicroEngineActorTemplate<DynamoLookupActor> {

    @Override
    protected Class<DynamoLookupActor> getDataSourceActorClz() {
        return DynamoLookupActor.class;
    }

    @Override
    protected void process(Response response) {
        MatchTraveler context = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() != null) {
            // got lattice account id from data source wrapper actor
            context.setResult(response.getResult());
            context.setMatched(true);
        }
    }

}
