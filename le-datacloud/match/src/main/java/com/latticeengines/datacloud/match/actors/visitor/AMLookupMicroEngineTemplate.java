package com.latticeengines.datacloud.match.actors.visitor;

import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class AMLookupMicroEngineTemplate extends DataSourceMicroEngineTemplate<DynamoLookupActor> {

    /**
     * @param keyTuple
     * @return information of used match key as a string which is used for
     *         traveler log
     */
    protected abstract String usedKeys(MatchKeyTuple keyTuple);

    @Override
    protected Class<DynamoLookupActor> getDataSourceActorClz() {
        return DynamoLookupActor.class;
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() != null) {
            AccountLookupEntry lookupEntry = (AccountLookupEntry) response.getResult();
            // got lattice account id from data source wrapper actor
            traveler.setResult((lookupEntry == null) ? null : lookupEntry.getLatticeAccountId());
            traveler.setMatched(true);
            traveler.debug(
                    "Found a precious LatticeAccountId=" + traveler.getLatticeAccountId() + " at "
                            + getClass().getSimpleName()
                            + " using " + usedKeys(traveler.getMatchKeyTuple()) + ", so ready to go home.");

            // $JAW$ Match Report
            traveler.addEntityMatchLookupResults(BusinessEntity.LatticeAccount.name() + "_AML",
                    Collections.singletonList(Pair.of(traveler.getMatchKeyTuple(),
                            Collections.singletonList(traveler.getLatticeAccountId()))));
            if (lookupEntry != null) {
                String logMessage = "The cacheId was " + lookupEntry.getId() + ".";
                if (lookupEntry.isPatched()) {
                    logMessage += " This lookup entry was manually patched.";
                }
                traveler.debug(logMessage);
            }
        } else {
            traveler.debug("Did not get any luck at " + getClass().getSimpleName() + " with "
                    + usedKeys(traveler.getMatchKeyTuple()));
        }
    }
}
