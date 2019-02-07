package com.latticeengines.datacloud.core.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

@Component
public class RateLimitingServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private RateLimitingService rateLimitingService;

    @Test(groups = "functional")
    public void testSimpleAcquisition() {
        RateLimitedAcquisition acquisition = rateLimitingService.acquireDnBBulkRequest(100L, true);
        Assert.assertNotNull(acquisition);

        acquisition = rateLimitingService.acquireDnBBulkRequest(100L, false);
        Assert.assertNotNull(acquisition);

        acquisition = rateLimitingService.acquireDnBBulkStatus(false);
        Assert.assertNotNull(acquisition);
    }

}
