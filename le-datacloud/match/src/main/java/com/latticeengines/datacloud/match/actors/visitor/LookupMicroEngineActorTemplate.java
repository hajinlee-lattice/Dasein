package com.latticeengines.datacloud.match.actors.visitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;

public abstract class LookupMicroEngineActorTemplate extends MicroEngineActorTemplate<DynamoLookupActor> {

    private static final Log log = LogFactory.getLog(LookupMicroEngineActorTemplate.class);

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
            context.debug(getClass().getSimpleName() + " found a lattice account id " + response.getResult() + " for " + context);
        } else {
            context.debug(getClass().getSimpleName() + " did not find any lattice account id for " + context);
        }
    }

}
