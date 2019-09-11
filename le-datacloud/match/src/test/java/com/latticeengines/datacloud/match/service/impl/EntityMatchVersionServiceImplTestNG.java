package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class EntityMatchVersionServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Tenant TEST_BASIC_OPERATION_TENANT = new Tenant("test_version_basic_operation");
    private static final Tenant TEST_CACHE_TENANT = new Tenant("test_version_cache");
    private static final Tenant TEST_SET_VERSION_TENANT = new Tenant("test_version_set_version");
    private static final List<Tenant> TEST_TENANTS = Arrays.asList(
            TEST_BASIC_OPERATION_TENANT, TEST_CACHE_TENANT, TEST_SET_VERSION_TENANT);

    @Inject
    private EntityMatchVersionServiceImpl entityMatchVersionService;

    @BeforeClass(groups = "functional")
    @AfterClass(groups = "functional")
    private void clearVersions() {
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            TEST_TENANTS.forEach(tenant -> entityMatchVersionService.clearVersion(env, tenant));
        }
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBasicOperations() {
        // clear version
        EntityMatchEnvironment env1 = EntityMatchEnvironment.STAGING;
        EntityMatchEnvironment env2 = EntityMatchEnvironment.SERVING;

        // get default version
        int defaultVersion1 = entityMatchVersionService.getCurrentVersion(env1, TEST_BASIC_OPERATION_TENANT);
        int nextVersion1 = entityMatchVersionService.getNextVersion(env1, TEST_BASIC_OPERATION_TENANT);
        int defaultVersion2 = entityMatchVersionService.getCurrentVersion(env2, TEST_BASIC_OPERATION_TENANT);
        int nextVersion2 = entityMatchVersionService.getNextVersion(env2, TEST_BASIC_OPERATION_TENANT);
        Assert.assertEquals(defaultVersion1, defaultVersion2); // different env should have the same default version
        Assert.assertEquals(nextVersion1, nextVersion2);
        Assert.assertNotEquals(defaultVersion1, nextVersion1);

        // bump version
        int bumpedVersion1 = entityMatchVersionService.bumpVersion(env1, TEST_BASIC_OPERATION_TENANT);
        int bumpedNextVersion1 = entityMatchVersionService.getNextVersion(env1, TEST_BASIC_OPERATION_TENANT);
        // check bumped
        Assert.assertEquals(bumpedVersion1, nextVersion1);
        Assert.assertNotEquals(bumpedVersion1, bumpedNextVersion1);
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env1, TEST_BASIC_OPERATION_TENANT), bumpedVersion1);
        // the other is not changed
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env2, TEST_BASIC_OPERATION_TENANT), defaultVersion2);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSetVersion() {
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        Tenant tenant = TEST_SET_VERSION_TENANT;

        int currVersion = entityMatchVersionService.getCurrentVersion(env, tenant);
        int nextVersion = entityMatchVersionService.getNextVersion(env, tenant);
        // test set version when there is no entry
        entityMatchVersionService.setVersion(env, tenant, currVersion);
        // out of range versions, should fail
        setInvalidVersion(env, tenant, currVersion - 1);
        setInvalidVersion(env, tenant, nextVersion);

        // bump up version a few times
        for (int i = 0; i < 5; i++) {
            entityMatchVersionService.bumpVersion(env, tenant);
        }

        nextVersion = entityMatchVersionService.getNextVersion(env, tenant);
        for (int version = currVersion; version < nextVersion; version++) {
            entityMatchVersionService.setVersion(env, tenant, version);

            Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env, tenant), version);
        }
        // out of range versions, should fail
        setInvalidVersion(env, tenant, currVersion - 1);
        setInvalidVersion(env, tenant, nextVersion);
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

    private void setInvalidVersion(EntityMatchEnvironment env, Tenant tenant, int version) {
        try {
            entityMatchVersionService.setVersion(env, tenant, version);
            Assert.fail(String.format("Should fail by setting invalid version %d for tenant %s in env %s", version,
                    tenant.getId(), env.name()));
        } catch (IllegalArgumentException e) {
            Assert.assertNotNull(e);
        }
    }

    private void verifyVersionInCache(EntityMatchEnvironment env, Tenant tenant, Integer expectedVersion) {
        Assert.assertNotNull(entityMatchVersionService.getVersionCache());
        Pair<String, EntityMatchEnvironment> cacheKey = Pair.of(tenant.getId(), env);
        Assert.assertEquals(entityMatchVersionService.getVersionCache().getIfPresent(cacheKey), expectedVersion);
    }
}
