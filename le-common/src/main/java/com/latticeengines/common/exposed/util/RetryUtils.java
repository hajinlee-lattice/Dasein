package com.latticeengines.common.exposed.util;

public class RetryUtils {

    private static final long INITIAL_WAIT_INTERVAL = 100L;

    public static long getExponentialWaitTime(int retryCount) {
        return retryCount == 0 ? 0 : ((long) Math.pow(2, retryCount) * INITIAL_WAIT_INTERVAL);
    }
}
