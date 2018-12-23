package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Inject;

public class EntityMatchVersionServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_TABLE = "CDLMatchServingDev_20181126";
    private static final Tenant TEST_TENANT = new Tenant("test_tenant_1");

    @Inject
    private EntityMatchVersionServiceImpl entityMatchVersionService;

    @BeforeClass(groups = "functional")
    private void setup() {
        // mock table
        entityMatchVersionService.setTableName(TEST_TABLE);
    }

    @Test(groups = "functional")
    private void testBasicOperations() {
        // clear version
        EntityMatchEnvironment env1 = EntityMatchEnvironment.STAGING;
        EntityMatchEnvironment env2 = EntityMatchEnvironment.SERVING;
        entityMatchVersionService.clearVersion(env1, TEST_TENANT);
        entityMatchVersionService.clearVersion(env2, TEST_TENANT);

        // get default version
        int defaultVersion1 = entityMatchVersionService.getCurrentVersion(env1, TEST_TENANT);
        int defaultVersion2 = entityMatchVersionService.getCurrentVersion(env2, TEST_TENANT);
        Assert.assertEquals(defaultVersion1, defaultVersion2); // different env should have the same default version

        // bump version
        int bumpedVersion1 = entityMatchVersionService.bumpVersion(env1, TEST_TENANT);
        // check bumped
        Assert.assertEquals(bumpedVersion1, defaultVersion1 + 1);
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env1, TEST_TENANT), bumpedVersion1);
        // the other is not changed
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env2, TEST_TENANT), defaultVersion2);

        // cleanup
        entityMatchVersionService.clearVersion(env1, TEST_TENANT);
        entityMatchVersionService.clearVersion(env2, TEST_TENANT);
    }
}
