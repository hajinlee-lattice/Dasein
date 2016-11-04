package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupService;
import com.latticeengines.domain.exposed.datacloud.match.DnBBulkMatchInfo;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchOutput;

@Component
public class DnBBulkLookupServiceImpl implements DnBBulkLookupService {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Override
    public DnBBulkMatchInfo sendRequest(List<MatchKeyTuple> input) {
        return null;
    }

    @Override
    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info) {
        return null;
    }

}
