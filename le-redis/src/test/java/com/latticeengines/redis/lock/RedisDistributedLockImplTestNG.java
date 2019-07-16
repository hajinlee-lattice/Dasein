package com.latticeengines.redis.lock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;

@TestExecutionListeners({DirtiesContextTestExecutionListener.class})
@ContextConfiguration(locations = {"classpath:test-redis-context.xml"})
public class RedisDistributedLockImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private RedisDistributedLock redisDistributedLock;

    private Tenant tenant1 = new Tenant();
    private Tenant tenant2 = new Tenant();

    @BeforeClass(groups = "functional")
    public void setup() {
        tenant1.setPid(1l);
        tenant2.setPid(2l);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {

    }

    @Test(groups = {"functional"})
    public void testLock() {
        String requestId1 = UUID.randomUUID().toString();
        String requestId2 = UUID.randomUUID().toString();
        assertEquals(redisDistributedLock.lock(tenant1.getPid().toString(), requestId1, 500, false), true);
        assertEquals(redisDistributedLock.lock(tenant1.getPid().toString(), requestId2, 500, false), false);
        assertEquals(redisDistributedLock.lock(tenant2.getPid().toString(), requestId2, 500, true), true);
        assertEquals(redisDistributedLock.get(tenant1.getPid().toString()), requestId1);
        assertEquals(redisDistributedLock.get(tenant2.getPid().toString()), requestId2);
        assertEquals(redisDistributedLock.releaseLock(tenant1.getPid().toString(), requestId2), false);
        assertEquals(redisDistributedLock.releaseLock(tenant1.getPid().toString(), requestId1), true);
        assertEquals(redisDistributedLock.releaseLock(tenant2.getPid().toString(), requestId2), true);
        assertTrue(StringUtils.isBlank(redisDistributedLock.get(tenant2.getPid().toString())));
        assertTrue(StringUtils.isBlank(redisDistributedLock.get(tenant1.getPid().toString())));
    }

}
