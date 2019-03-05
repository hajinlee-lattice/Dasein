package com.latticeengines.camille.locks;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;

public class LockManagerUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(LockManagerUnitTestNG.class);

    @BeforeClass(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testLock() {
        final String lockName = "TestLock";
        LockManager.registerDivisionPrivateLock(lockName);

        LockManager.acquireWriteLock(lockName, 1, TimeUnit.SECONDS);
        try {
            LockManager.upsertData(lockName, "1", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            Assert.fail("Failed to upsert lock data", e);
        }
        LockManager.releaseWriteLock(lockName);

        String data = "";
        try {
            data = LockManager.peekData(lockName, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("Failed to peek data", e);
        }
        Assert.assertEquals(data, "1");

        LockManager.acquireWriteLock(lockName, 1, TimeUnit.SECONDS);
        try {
            LockManager.upsertData(lockName, "2", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            Assert.fail("Failed to upsert lock data", e);
        }
        LockManager.releaseWriteLock(lockName);

        try {
            data = LockManager.peekData(lockName, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("Failed to peek data", e);
        }
        Assert.assertEquals(data, "2");
    }

    @Test(groups = "unit")
    public void testEagerRead() {
        final String lockName = "TestEagerRead";
        LockManager.registerDivisionPrivateLock(lockName);

        LockManager.acquireWriteLock(lockName, 1, TimeUnit.SECONDS);
        try {
            LockManager.upsertData(lockName, "1", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            Assert.fail("Failed to upsert lock data", e);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<Boolean> callable = () -> {
            String data = "";
            try {
                data = LockManager.peekData(lockName, 1, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to peek data", e);
                return false;
            }
            return "1".equals(data);
        };
        Future<Boolean> future = executor.submit(callable);

        try {
            Assert.assertFalse(future.get(3, TimeUnit.SECONDS), "Should not get the read lock");
        } catch (Exception e) {
            Assert.fail("Failed to wait for the thread", e);
        }

        String data = "";
        try {
            data = LockManager.peekData(lockName, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("Failed to peek data", e);
        }
        Assert.assertEquals(data, "1");

        LockManager.releaseWriteLock(lockName);

        executor = Executors.newSingleThreadExecutor();
        future = executor.submit(callable);

        try {
            Assert.assertTrue(future.get(3, TimeUnit.SECONDS), "Should be able to get the data");
        } catch (Exception e) {
            Assert.fail("Failed to wait for the thread", e);
        }
    }

    @Test(groups = "unit")
    public void testBadWrite() {
        final String lockName = "TestBadWrite";
        boolean exception;
        LockManager.registerDivisionPrivateLock(lockName);

        exception = false;
        try {
            LockManager.upsertData(lockName, "1", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            exception = true;
        }
        Assert.assertTrue(exception, "Should have thrown exception.");

        LockManager.acquireWriteLock(lockName, 1, TimeUnit.SECONDS);
        try {
            LockManager.upsertData(lockName, "1", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            Assert.fail("Failed to upsert lock data", e);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(() -> LockManager.acquireWriteLock(lockName, 1, TimeUnit.SECONDS));
        try {
            Assert.assertFalse(future.get(3, TimeUnit.SECONDS), "Should not get the write lock");
        } catch (Exception e) {
            Assert.fail("Failed to wait for the thread", e);
        }

        LockManager.releaseWriteLock(lockName);

        exception = false;
        try {
            LockManager.upsertData(lockName, "1", CamilleEnvironment.getDivision());
        } catch (Exception e) {
            exception = true;
        }
        Assert.assertTrue(exception, "Should have thrown exception.");
    }
}
