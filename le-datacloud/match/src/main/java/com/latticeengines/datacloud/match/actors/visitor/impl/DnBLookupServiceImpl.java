package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        return dnbRealTimeLookupService.realtimeEntityLookup(matchKeyTuple);
    }
}
