package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Override
    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String result = null;
        if (request.getInputData() instanceof MatchKeyTuple) {
            MatchKeyTuple input = (MatchKeyTuple) request.getInputData();
            result = UUID.randomUUID().toString();
            log.debug("Got result from lookup for = " + input + " lookupRequestId = " + lookupRequestId
                    + ", result DUNS = " + result);
        }
        return result;
    }
}
