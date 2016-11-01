package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Override
    protected String lookupFromService(DataSourceLookupRequest request) {
        String result = null;
        if (request.getInputData() instanceof Map) {
            // TODO: call DnB lookup
            log.info("Got result from lookup for =");
        }
        return result;
    }
}
