package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.EntityMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class EntityMatchVersionServiceImplTestNG extends EntityMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchVersionServiceImplTestNG.class);

    @Inject
    private EntityMatchVersionServiceImpl entityMatchVersionService;

    private Queue<Tenant> tenants = new ConcurrentLinkedQueue<>();

    @AfterClass(groups = "functional")
    private void clearVersions() {
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            tenants.forEach(tenant -> entityMatchVersionService.clearVersion(env, tenant));
        }
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBasicOperations() {
        Tenant tenant = newTestTenant();
        tenants.add(tenant);
        // clear version
        EntityMatchEnvironment env1 = EntityMatchEnvironment.STAGING;
        EntityMatchEnvironment env2 = EntityMatchEnvironment.SERVING;

        // get default version
        int defaultVersion1 = entityMatchVersionService.getCurrentVersion(env1, tenant);
        int nextVersion1 = entityMatchVersionService.getNextVersion(env1, tenant);
        int defaultVersion2 = entityMatchVersionService.getCurrentVersion(env2, tenant);
        int nextVersion2 = entityMatchVersionService.getNextVersion(env2, tenant);
        Assert.assertEquals(defaultVersion1, defaultVersion2); // different env should have the same default version
        Assert.assertEquals(nextVersion1, nextVersion2);
        Assert.assertNotEquals(defaultVersion1, nextVersion1);

        // bump version
        int bumpedVersion1 = entityMatchVersionService.bumpVersion(env1, tenant);
        int bumpedNextVersion1 = entityMatchVersionService.getNextVersion(env1, tenant);
        // check bumped
        Assert.assertEquals(bumpedVersion1, nextVersion1);
        Assert.assertNotEquals(bumpedVersion1, bumpedNextVersion1);
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env1, tenant), bumpedVersion1);
        // the other is not changed
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env2, tenant), defaultVersion2);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSetVersion() {
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        Tenant tenant = newTestTenant();
        tenants.add(tenant);

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

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBumpNextVersion() {
        EntityMatchEnvironment env = EntityMatchEnvironment.SERVING;
        Tenant tenant = newTestTenant();
        tenants.add(tenant);

        int currVersion = entityMatchVersionService.getCurrentVersion(env, tenant);
        int nextVersion = entityMatchVersionService.getNextVersion(env, tenant);

        int bumpedNextVersion = entityMatchVersionService.bumpNextVersion(env, tenant);

        Assert.assertNotEquals(bumpedNextVersion, nextVersion);
        // current version is not changed
        Assert.assertEquals(entityMatchVersionService.getCurrentVersion(env, tenant), currVersion);
        Assert.assertEquals(entityMatchVersionService.getNextVersion(env, tenant), bumpedNextVersion);
    }

    /*
     * make sure in-memory cache for version is working properly
     */
    @Test(groups = "functional")
    private void testCache() {
        Tenant tenant = newTestTenant();
        tenants.add(tenant);
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

    @Override
    protected List<String> getExpectedOutputColumns() {
        return Collections.emptyList();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
