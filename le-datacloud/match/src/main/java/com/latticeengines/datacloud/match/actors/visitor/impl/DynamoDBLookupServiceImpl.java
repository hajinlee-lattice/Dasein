package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@Component("dynamoDBLookupService")
public class DynamoDBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DynamoDBLookupServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Override
    protected String lookupFromService(DataSourceLookupRequest request) {
        String result = null;
        if (request.getInputData() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> inputMap = (Map<String, Object>) request.getInputData();
            String lookupId = AccountLookupEntry.buildId((String) inputMap.get(MatchKey.Domain.name()),
                    (String) inputMap.get(MatchKey.DUNS.name()));
            AccountLookupEntryMgr lookupMgr = accountLookupService.getLookupMgr(request.getMatchTravelerContext()
                    .getDataCloudVersion());
            AccountLookupEntry lookupEntry = lookupMgr.findByKey(lookupId);
            if (lookupEntry != null) {
                result = lookupEntry.getLatticeAccountId();
            }
            if (result != null) {
                log.info("Got result from lookup for Lookup key=" + lookupId + " Lattice Account Id=" + result);
            }
        }
        return result;

    }
}
