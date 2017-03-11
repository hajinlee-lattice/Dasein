package com.latticeengines.camille.exposed.locks;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class LockManager {

    private static final ConcurrentMap<String, InterProcessReadWriteLock> locks = new ConcurrentHashMap<>();
    private static Log log = LogFactory.getLog(LockManager.class);
    private static final ConcurrentSkipListSet<String> privateLocks = new ConcurrentSkipListSet<>();

    public static void registerDivisionPrivateLock(String lockName) {
        registerLock(lockName, CamilleEnvironment.getDivision());
        privateLocks.add(lockName);
    }

    public static void registerCrossDivisionLock(String lockName) {
        registerLock(lockName, "");
        privateLocks.remove(lockName); // Remove if present
    }

    private static void registerLock(String lockName, String division) {
        if (!locks.containsKey(lockName)) {
            Path lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), division, lockName);
            InterProcessReadWriteLock newLock = CamilleEnvironment.getCamille().createLock(lockPath.toString());
            locks.putIfAbsent(lockName, newLock);
            log.info("Registered a new lock " + lockName + " at " + lockPath);
        }
    }

    // this method is only for testing
    public static void deregisterDivisionPrivateLock(String lockName) {
        deregisterLock(lockName, CamilleEnvironment.getDivision());
        privateLocks.remove(lockName);
    }

    // this method is only for testing
    public static void deregisterCrossDivisionLock(String lockName) {
        deregisterLock(lockName, "");
    }

    private static void deregisterLock(String lockName, String dvision) {
        if (locks.containsKey(lockName)) {
            log.info("Deregistering the lock " + lockName);
            locks.remove(lockName);
        }
        Path lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), dvision, lockName);
        try {
            if (CamilleEnvironment.getCamille().exists(lockPath)) {
                CamilleEnvironment.getCamille().delete(lockPath);
            }
        } catch (Exception e) {
            log.error("Error deleting lock path " + lockPath);
        }
    }

    public static String peekData(String lockName, long duration, TimeUnit timeUnit) throws Exception {
        String division = "";
        if (privateLocks.contains(lockName)) {
            division = CamilleEnvironment.getDivision();
        }
        Path lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), division,
                lockName);
        Camille camille = CamilleEnvironment.getCamille();
        InterProcessReadWriteLock lock = locks.get(lockName);
        if (lock == null) {
            throw new Exception("Lock " + lockName + " is not registered");
        }

        try {
            if (lock.readLock().acquire(duration, timeUnit)) {
                if (lock.readLock().isAcquiredInThisProcess()) {
                    if (camille.exists(lockPath)) {
                        return camille.get(lockPath).getData();
                    } else {
                        throw new Exception("The lock path " + lockPath + " does not exist.");
                    }
                } else {
                    throw new Exception(
                            "Current thread is suppose to have the read lock " + lockName + ", but it does not.");
                }
            } else {
                throw new Exception("Failed to acquire read lock " + lockName);
            }
        } catch (Exception e) {
            throw new Exception("Failed to peek data at lock " + lockName, e);
        } finally {
            if (lock.readLock().isAcquiredInThisProcess()) {
                try {
                    lock.readLock().release();
                } catch (Exception e) {
                    log.error("Exception when acquiring read lock " + lockName, e);
                }
            }
        }
    }

    public static boolean acquireWriteLock(String lockName, long duration, TimeUnit timeUnit) {
        InterProcessReadWriteLock lock = locks.get(lockName);
        if (lock == null) {
            log.warn("Lock " + lockName + " is not registered");
            return false;
        }
        try {
            if (lock.writeLock().acquire(duration, timeUnit)) {
                if (lock.writeLock().isAcquiredInThisProcess()) {
                    return true;
                } else {
                    throw new Exception(
                            "Current thread is suppose to have the write lock " + lockName + ", but it does not.");
                }
            } else {
                throw new Exception("Failed to acquire write lock " + lockName);
            }
        } catch (Exception e) {
            log.error("Exception when acquiring write lock " + lockName, e);
            if (lock.writeLock().isAcquiredInThisProcess()) {
                try {
                    lock.writeLock().release();
                } catch (Exception e1) {
                    log.error("Exception when releasing write lock " + lockName, e1);
                }
            }
            return false;
        }
    }

    public static void releaseWriteLock(String lockName) {
        InterProcessReadWriteLock lock = locks.get(lockName);
        if (lock == null) {
            log.warn("Lock " + lockName + " is not registered");
            return;
        }
        if (lock.writeLock().isAcquiredInThisProcess()) {
            try {
                lock.writeLock().release();
            } catch (Exception e) {
                log.error("Exception when acquiring write lock " + lockName, e);
            }
        }
    }

    public static void upsertData(String lockName, String data, String division) throws Exception {
        Path lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), division,
                lockName);
        Camille camille = CamilleEnvironment.getCamille();
        InterProcessReadWriteLock lock = locks.get(lockName);
        if (lock == null) {
            throw new Exception("Lock " + lockName + " is not registered");
        }
        try {
            if (lock.writeLock().isAcquiredInThisProcess()) {
                if (camille.exists(lockPath)) {
                    camille.upsert(lockPath, new Document(data), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                } else {
                    throw new Exception("The lock path " + lockPath + " does not exist.");
                }
            } else {
                throw new Exception(
                        "Current thread is suppose to have the write lock " + lockName + ", but it does not.");
            }
        } catch (Exception e) {
            throw new Exception("Failed to upsert data at lock " + lockName, e);
        }
    }

}
