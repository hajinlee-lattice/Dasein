package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupService;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;

@Component
public class DnBBulkLookupServiceImpl implements DnBBulkLookupService {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImpl.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Override
    public List<DnBMatchEntry> bulkEntitiesLookup(List<DnBMatchEntry> input) {
        return null;
    }

    @Override
    public List<DnBMatchEntry> bulkEmailsLookup(List<DnBMatchEntry> input) {
        return null;
    }
}
