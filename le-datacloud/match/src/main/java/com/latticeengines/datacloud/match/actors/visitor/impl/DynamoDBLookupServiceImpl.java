package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;

@Component("dynamoDBLookupService")
public class DynamoDBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DynamoDBLookupServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String result = null;
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();

        String lookupId = AccountLookupEntry.buildId(matchKeyTuple.getDomain(), matchKeyTuple.getDuns());

        if (matchKeyTuple.getDuns() != null || matchKeyTuple.getDomain() != null) {
            AccountLookupEntryMgr lookupMgr = accountLookupService
                    .getLookupMgr(request.getMatchTravelerContext().getDataCloudVersion());

            AccountLookupEntry lookupEntry = lookupMgr.findByKey(lookupId);
            if (lookupEntry != null) {
                result = lookupEntry.getLatticeAccountId();
            } else {
                log.debug("Didn't get anything from real dynamodb for " + lookupRequestId);
            }
        } else {
            log.debug("Skip lookup into dynamodb for " + lookupRequestId);
        }

        if (result != null) {
            log.debug("Got result from lookup for Lookup key=" + lookupId + " Lattice Account Id=" + result);
        }

        return result;
    }
}
