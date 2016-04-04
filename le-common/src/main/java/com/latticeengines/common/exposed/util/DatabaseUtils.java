package com.latticeengines.common.exposed.util;

import org.apache.commons.collections.Closure;
import org.apache.log4j.Logger;
import org.hibernate.exception.LockAcquisitionException;

public class DatabaseUtils {

    private static Logger log = Logger.getLogger(DatabaseUtils.class);

    public static void retry(String operationName, Closure action) {
        retry(operationName, 10, action);
    }

    public static void retry(String operationName, int retries, Closure action) {
        Exception thrown = null;
        for (int i = 0; i < retries; ++i) {
            long sleep = RetryUtils.getExponentialWaitTime(i);
            try {
                action.execute(null);
                return;
            } catch (LockAcquisitionException exception) {
                log.warn(String.format("Deadlock detected performing %s", operationName), exception);
                thrown = exception;
                if (i != retries - 1) {
                    log.warn(String.format("Sleeping for %d milliseconds and retrying", sleep));
                    sleep(sleep);
                }
            }
        }
        throw new RuntimeException(String.format("Could not perform operation %s after %d retries", operationName,
                retries), thrown);
    }

    private static void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
