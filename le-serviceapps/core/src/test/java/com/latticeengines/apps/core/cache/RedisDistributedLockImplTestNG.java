package com.latticeengines.apps.core.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.testframework.RedisLockImplTestNGBase;

public class RedisDistributedLockImplTestNG extends RedisLockImplTestNGBase {

    @Inject
    private RedisDistributedLock redisDistributedLock;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {

    }

    @Test(groups = {"functional"})
    public void testLock() {
        String requestId1 = UUID.randomUUID().toString();
        String requestId2 = UUID.randomUUID().toString();
        assertEquals(redisDistributedLock.lock(tenant1.getPid().toString(), requestId1, false), true);
        assertEquals(redisDistributedLock.lock(tenant1.getPid().toString(), requestId2, false), false);
        assertEquals(redisDistributedLock.lock(tenant2.getPid().toString(), requestId2, true), true);
        assertEquals(redisDistributedLock.get(tenant1.getPid().toString()), requestId1);
        assertEquals(redisDistributedLock.get(tenant2.getPid().toString()), requestId2);
        assertEquals(redisDistributedLock.releaseLock(tenant1.getPid().toString(), requestId2), false);
        assertEquals(redisDistributedLock.releaseLock(tenant1.getPid().toString(), requestId1), true);
        assertEquals(redisDistributedLock.releaseLock(tenant2.getPid().toString(), requestId2), true);
        assertTrue(StringUtils.isBlank(redisDistributedLock.get(tenant2.getPid().toString())));
        assertTrue(StringUtils.isBlank(redisDistributedLock.get(tenant1.getPid().toString())));
    }

}
