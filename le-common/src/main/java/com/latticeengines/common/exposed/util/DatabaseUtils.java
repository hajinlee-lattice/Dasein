package com.latticeengines.common.exposed.util;

import org.apache.commons.collections.Closure;
import org.hibernate.exception.LockAcquisitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseUtils {

    private static Logger log = LoggerFactory.getLogger(DatabaseUtils.class);

    public static void retry(String operationName, Closure action) {
        retry(operationName, 10, action);
    }

    public static void retry(String operationName, int retries, Closure action) {
        retry(operationName, retries, null, "Deadlock detected performing", null, action);
    }

    public static void retry(String operationName, int retries, Class<? extends Exception> customExceptionClazz,
            String customMessage, String customExcetionMessageSubstr, Closure action) {
        Class<? extends Exception> defaultExceptionClazz = LockAcquisitionException.class;
        String defaultMessage = "Deadlock detected performing";

        Exception thrown = null;
        for (int i = 0; i < retries; ++i) {
            long sleep = RetryUtils.getExponentialWaitTime(i);
            try {
                action.execute(null);
                return;
            } catch (Exception exception) {
                boolean shouldRetry = false;

                if (customExceptionClazz != null //
                        && customExceptionClazz.isAssignableFrom(exception.getClass())) {
                    if (StringStandardizationUtils.objectIsNullOrEmptyString(customExcetionMessageSubstr) //
                            || exception.getMessage().trim().toLowerCase()
                                    .contains(customExcetionMessageSubstr.trim().toLowerCase())) {
                        log.warn(String.format("%s %s", customMessage, operationName));
                        shouldRetry = true;
                    }
                } else if (defaultExceptionClazz != null//
                        && defaultExceptionClazz.isAssignableFrom(exception.getClass())) {
                    log.warn(String.format("%s %s", defaultMessage, operationName));
                    shouldRetry = true;
                }

                if (shouldRetry) {
                    thrown = exception;
                    if (i != retries - 1) {
                        log.warn(String.format("Sleeping for %d milliseconds and retrying", sleep));
                        sleep(sleep);
                    }
                } else {
                    throw exception;
                }
            }
        }
        throw new RuntimeException(
                String.format("Could not perform operation %s after %d retries", operationName, retries), thrown);
    }

    private static void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
