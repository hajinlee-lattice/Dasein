package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Inject;

public class CDLMatchVersionServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_TABLE = "CDLMatchServingDev_20181126";
    private static final Tenant TEST_TENANT = getTestTenant();

    @Inject
    private CDLMatchVersionServiceImpl cdlMatchVersionService;

    @BeforeClass(groups = "functional")
    private void setup() {
        // mock table
        cdlMatchVersionService.setTableName(TEST_TABLE);
    }

    @Test(groups = "functional")
    private void testBasicOperations() {
        // clear version
        CDLMatchEnvironment env1 = CDLMatchEnvironment.STAGING;
        CDLMatchEnvironment env2 = CDLMatchEnvironment.SERVING;
        cdlMatchVersionService.clearVersion(env1, TEST_TENANT);
        cdlMatchVersionService.clearVersion(env2, TEST_TENANT);

        // get default version
        int defaultVersion1 = cdlMatchVersionService.getCurrentVersion(env1, TEST_TENANT);
        int defaultVersion2 = cdlMatchVersionService.getCurrentVersion(env2, TEST_TENANT);
        Assert.assertEquals(defaultVersion1, defaultVersion2); // different env should have the same default version

        // bump version
        int bumpedVersion1 = cdlMatchVersionService.bumpVersion(env1, TEST_TENANT);
        // check bumped
        Assert.assertEquals(bumpedVersion1, defaultVersion1 + 1);
        Assert.assertEquals(cdlMatchVersionService.getCurrentVersion(env1, TEST_TENANT), bumpedVersion1);
        // the other is not changed
        Assert.assertEquals(cdlMatchVersionService.getCurrentVersion(env2, TEST_TENANT), defaultVersion2);

        // cleanup
        cdlMatchVersionService.clearVersion(env1, TEST_TENANT);
        cdlMatchVersionService.clearVersion(env2, TEST_TENANT);
    }


    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("test_tenant_1");
        tenant.setPid(1L);
        return tenant;
    }
}
