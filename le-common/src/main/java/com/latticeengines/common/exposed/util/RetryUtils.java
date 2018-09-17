package com.latticeengines.common.exposed.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public class RetryUtils {

    private static final long INITIAL_WAIT_INTERVAL = 100L;

    public static long getExponentialWaitTime(int retryCount) {
        return retryCount == 0 ? 0 : ((long) Math.pow(2, retryCount) * INITIAL_WAIT_INTERVAL);
    }

    public static RetryTemplate getRetryTemplate(int maxAttempts) {
        return getRetryTemplate(maxAttempts, null, null);
    }

    public static RetryTemplate getRetryTemplate(int maxAttempts, //
            Collection<Class<? extends Throwable>> retryExceptions, //
            Collection<Class<? extends Throwable>> stopExceptions) {
        Map<Class<? extends Throwable>, Boolean> retryExceptionMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(retryExceptions)) {
            for (Class<? extends Throwable> t : retryExceptions) {
                retryExceptionMap.put(t, Boolean.TRUE);
            }
        }
        if (CollectionUtils.isNotEmpty(retryExceptions)) {
            for (Class<? extends Throwable> t : stopExceptions) {
                retryExceptionMap.put(t, Boolean.FALSE);
            }
        }
        return getExponentialBackoffRetryTemplate(maxAttempts, 2000L, 2.0D, retryExceptionMap);
    }

    public static RetryTemplate getExponentialBackoffRetryTemplate(int maxAttempts,
            long initialWaitMsec, double multiplier,
            Map<Class<? extends Throwable>, Boolean> retryExceptionMap) {
        RetryTemplate template = new RetryTemplate();
        SimpleRetryPolicy retryPolicy;
        if (MapUtils.isNotEmpty(retryExceptionMap)) {
            retryPolicy = new SimpleRetryPolicy(maxAttempts, retryExceptionMap, true);
        } else {
            retryPolicy = new SimpleRetryPolicy(maxAttempts);
        }
        template.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        template.setBackOffPolicy(backOffPolicy);
        template.setThrowLastExceptionOnExhausted(true);
        return template;
    }
}
