package com.latticeengines.testframework.exposed.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestControlUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestControlUtils.class);
    private static final Integer DEFAULT_MAX_ATTEMPTS = 5;
    private static final Integer DEFAULT_WAIT_INTERVAL = 3;

    protected TestControlUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T> T wait(Supplier<T> supplier, T expectValue, int interval, int maxWait, TimeUnit timeUnit,
            Function<T, Boolean> interrupter) throws TimeoutException {
        final int maxWait1 = maxWait;
        if (null == interrupter) {
            interrupter = t -> false;
        }
        do {
            T result = supplier.get();
            if (result.equals(expectValue)) {
                return result;
            } else if (interrupter.apply(result)) {
                logger.warn("wait is interrupted, return {}", result);
                return result;
            } else {
                try {
                    Thread.sleep(timeUnit.toMillis(interval));
                } catch (InterruptedException e) {
                    logger.warn("Interception error happens, ignore it");
                } finally {
                    logger.info("expect value: {}, actual value after {} {}: {}", expectValue, maxWait1 - maxWait,
                            timeUnit, result);
                    maxWait -= interval;
                }
            }
        } while (maxWait > 0);
        logger.error("Expect value {} is not reached after waiting {} {}", expectValue.toString(), maxWait1, timeUnit);
        throw new TimeoutException();
    }

    public static <T> T defaultWait(Supplier<T> supplier, T expectValue, int maxWait) throws TimeoutException {
        return wait(supplier, expectValue, DEFAULT_WAIT_INTERVAL, maxWait, TimeUnit.MINUTES, null);
    }

    public static <T> T defaultWait(Supplier<T> supplier, T expectValue, int maxWait, Function<T, Boolean> interrupter)
            throws TimeoutException {
        return wait(supplier, expectValue, DEFAULT_WAIT_INTERVAL, maxWait, TimeUnit.MINUTES, interrupter);
    }

    public static <T> T retry(Supplier<T> supplier, T whatForRetry, int interval, TimeUnit timeUnit) {
        return retry(DEFAULT_MAX_ATTEMPTS, supplier, whatForRetry, interval, timeUnit);
    }

    public static <T> T retry(int maxAttempts, Supplier<T> supplier, T whatForRetry, int interval, TimeUnit timeUnit) {
        for (int i = maxAttempts; i > 0; --i) {
            T t = supplier.get();
            if (t == null || t.equals(whatForRetry)) {
                try {
                    Thread.sleep(timeUnit.toMillis(interval));
                } catch (InterruptedException ignored) {
                }
            } else {
                return t;
            }
        }
        return whatForRetry;
    }
}
