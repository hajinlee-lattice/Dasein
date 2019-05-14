package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

public class EntityMatchVersionServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Tenant TEST_BASIC_OPERATION_TENANT = new Tenant("test_version_basic_operation");
    private static final Tenant TEST_CACHE_TENANT = new Tenant("test_version_cache");
    private static final List<Tenant> TEST_TENANTS = Arrays.asList(
            TEST_BASIC_OPERATION_TENANT, TEST_CACHE_TENANT);

    @Inject
    private EntityMatchVersionServiceImpl entityMatchVersionService;

    @BeforeClass(groups = "functional")
    @AfterClass(groups = "functional")
    private void clearVersions() {
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            TEST_TENANTS.forEach(tenant -> entityMatchVersionService.clearVersion(env, tenant));
        }
    }

    @Test(groups = "functional")
    private void testBasicOperations() {
        // clear version
        EntityMatchEnvironment env1 = EntityMatchEnvironment.STAGING;
        EntityMatchEnvironment env2 = EntityMatchEnvironment.SERVING;

        // get default version
        int defaultVersion1 = entityMatchVersionService.getCurrentVersion(env1, TEST_BASIC_OPERATION_TENANT);
        int defaultVersion2 = entityMatchVersionService.getCurrentVersion(env2, TEST_BASIC_OPERATION_TENANT);
        Assert.assertEquals(defaultVersion1, defaultVersion2); // different env should have the same default version

        // bump version
        int bumpedVersion1 = entityMatchVersionService.bumpVersion(env1, TEST_BASIC_OPERATION_TENANT);
        // check bumped
        Assert.assertEquals(bumpedVersion1, defaultVersion1 + 1);
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env1, TEST_BASIC_OPERATION_TENANT), bumpedVersion1);
        // the other is not changed
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env2, TEST_BASIC_OPERATION_TENANT), defaultVersion2);
    }

    /*
     * make sure in-memory cache for version is working properly
     */
    @Test(groups = "functional")
    private void testCache() {
        Tenant tenant = TEST_CACHE_TENANT;
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            int v1 = entityMatchVersionService.getCurrentVersion(env, tenant);
            // check the value is cache is correct
            verifyVersionInCache(env, tenant, v1);
            entityMatchVersionService.bumpVersion(env, tenant);

            // get again to refresh the cache
            int v2 = entityMatchVersionService.getCurrentVersion(env, tenant);
            // check value again after bumping the version
            Assert.assertEquals(v1 + 1, v2);
            verifyVersionInCache(env, tenant, v2);
        }
    }

    private void verifyVersionInCache(EntityMatchEnvironment env, Tenant tenant, Integer expectedVersion) {
        Assert.assertNotNull(entityMatchVersionService.getVersionCache());
        Pair<String, EntityMatchEnvironment> cacheKey = Pair.of(tenant.getId(), env);
        Assert.assertEquals(entityMatchVersionService.getVersionCache().getIfPresent(cacheKey), expectedVersion);
    }
}
