package com.latticeengines.datacloud.match.service.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBulkMatchInfo;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;

@Component
public class DnBBulkLookupServiceImpl implements DnBBulkLookupService {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    // Key in the input is lookupRequestId.
    // Put it in the TransactionalId field (2nd field) in the bulk match input
    @Override
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input) {
        return null;
    }

    // Key in the output is lookupRequestId.
    // Get it from the TransactionalId field in the bulk match output
    @Override
    public Map<String, DnBMatchOutput> getResult(DnBBulkMatchInfo info) {
        return null;
    }

}
