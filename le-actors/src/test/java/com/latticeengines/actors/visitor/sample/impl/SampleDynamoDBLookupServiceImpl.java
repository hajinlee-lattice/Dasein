package com.latticeengines.actors.visitor.sample.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupRequest;
import com.latticeengines.actors.visitor.sample.SampleMatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;

@Component("sampleDynamoDBLookupService")
public class SampleDynamoDBLookupServiceImpl extends SampleDataSourceLookupServiceBase {
    private static final Logger log = LoggerFactory.getLogger(SampleDynamoDBLookupServiceImpl.class);

    protected String lookupFromService(String lookupRequestId, SampleDataSourceLookupRequest request) {
        String result = null;
        SampleMatchKeyTuple matchKeyTuple = (SampleMatchKeyTuple) request.getInputData();

        String lookupId = AccountLookupEntry.buildId(matchKeyTuple.getDomain(), matchKeyTuple.getDuns());

        if (matchKeyTuple.getDuns() != null && matchKeyTuple.getDomain() != null) {
            result = "AM_KEY_" + UUID.randomUUID().toString();

        } else {
            log.debug("Skip lookup into dynamodb for " + lookupRequestId);
        }

        if (result != null) {
            log.debug("Got result from lookup for Lookup key=" + lookupId + " Lattice Account Id=" + result);
        }

        return result;
    }
}
