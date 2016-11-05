package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;

@Component("dynamoDBLookupService")
public class DynamoDBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DynamoDBLookupServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String result = null;
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();

        if (matchKeyTuple.getDuns() != null || matchKeyTuple.getDomain() != null) {
            AccountLookupRequest accountLookupRequest = new AccountLookupRequest(
                    request.getMatchTravelerContext().getDataCloudVersion());
            accountLookupRequest.addLookupPair(matchKeyTuple.getDomain(), matchKeyTuple.getDuns());
            result = accountLookupService.batchLookupIds(accountLookupRequest).get(0);
            if (StringUtils.isNotEmpty(result)) {
                log.debug("Got result from lookup for Lookup key=" + accountLookupRequest.getIds().get(0)
                        + " Lattice Account Id=" + result);
            } else {
                // may not be able to handle empty string
                result = null;
                log.debug("Didn't get anything from dynamodb for " + lookupRequestId);
            }
        } else {
            log.debug("Skip lookup into dynamodb for " + lookupRequestId);
        }

        return result;
    }
}
