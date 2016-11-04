package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class DnBBulkLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImplTestNG.class);

    @Autowired
    private DnBBulkLookupService dnBBulkLookupService;

    @Test(groups = "functional", enabled = false)
    public void testRealTimeLookupService() {
        List<MatchKeyTuple> input = new ArrayList<MatchKeyTuple>();
        dnBBulkLookupService.sendRequest(input);
    }
}
