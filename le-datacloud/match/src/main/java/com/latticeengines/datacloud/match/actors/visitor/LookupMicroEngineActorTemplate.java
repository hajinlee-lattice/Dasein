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
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() != null) {
            // got lattice account id from data source wrapper actor
            traveler.setResult(response.getResult());
            traveler.setMatched(true);
            traveler.debug(
                    "Found a precious LatticeAccountId=" + response.getResult() + " at " + getClass().getSimpleName()
                            + " using " + usedKeys(traveler.getMatchKeyTuple()) + ", so ready to go home.");
        } else {
            traveler.debug("Did not get any luck at " + getClass().getSimpleName() + " with "
                    + usedKeys(traveler.getMatchKeyTuple()));
        }
    }

    protected abstract String usedKeys(MatchKeyTuple keyTuple);
}
