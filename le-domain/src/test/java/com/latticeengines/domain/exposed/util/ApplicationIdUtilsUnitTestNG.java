package com.latticeengines.domain.exposed.util;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ApplicationIdUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "expandTestData")
    public void testExpandAndStrip(String jobId, String applicationId) {
        Assert.assertEquals(ApplicationIdUtils.expandToApplicationId(jobId), applicationId, jobId);
        Assert.assertEquals(ApplicationIdUtils.stripJobId(applicationId), jobId, applicationId);
    }

    @DataProvider(name = "expandTestData")
    public Object[][] provideExpandTestData() {
        String uuid = UUID.randomUUID().toString();
        return new Object[][] {
                { "1546400511805_16000", "application_1546400511805_16000" },
                { "1546400511805_0001", "application_1546400511805_0001" },
                { "9b81e92a-2297-4da5-bc3a-b73020e1f139", "application_9b81e92a-2297-4da5-bc3a-b73020e1f139_aws" },
                { uuid, "application_" + uuid + "_aws" }
        };
    }

}
