package com.latticeengines.actors.visitor.sample.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupRequest;
import com.latticeengines.actors.visitor.sample.SampleMatchKeyTuple;

@Component("sampleDnBLookupService")
public class SampleDnBLookupServiceImpl extends SampleDataSourceLookupServiceBase {
    private static final Logger log = LoggerFactory.getLogger(SampleDnBLookupServiceImpl.class);

    @Override
    protected String lookupFromService(String lookupRequestId, SampleDataSourceLookupRequest request) {
        String result = null;
        if (request.getInputData() instanceof SampleMatchKeyTuple) {
            SampleMatchKeyTuple input = (SampleMatchKeyTuple) request.getInputData();
            result = UUID.randomUUID().toString();
            log.debug("Got result from lookup for = " + input + " lookupRequestId = " + lookupRequestId
                    + ", result DUNS = " + result);
        }
        return result;
    }
}
