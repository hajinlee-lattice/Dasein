package com.latticeengines.domain.exposed.aws;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AwsApplicationIdUnitTestNG {

    @Test(groups = "unit")
    public void testIsAwsBatchJob() {
        String uuid = UUID.randomUUID().toString();
        String appId = "application_" + uuid + "_aws";
        Assert.assertFalse(AwsApplicationId.isAwsBatchJob(appId), appId);

        appId = "application_1546931484138_12140_aws";
        Assert.assertFalse(AwsApplicationId.isAwsBatchJob(appId));
    }

}
