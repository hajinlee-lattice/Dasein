package com.latticeengines.common.exposed.util;

import java.util.Map;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public class RetryUtils {

    private static final long INITIAL_WAIT_INTERVAL = 100L;

    public static long getExponentialWaitTime(int retryCount) {
        return retryCount == 0 ? 0 : ((long) Math.pow(2, retryCount) * INITIAL_WAIT_INTERVAL);
    }

    public static RetryTemplate getExponentialBackoffRetryTemplate(
            int maxAttempts, long initialWaitMsec, double multiplier, Map<Class<? extends Throwable>, Boolean> retryExceptionMap) {
        RetryTemplate template = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, retryExceptionMap, true);
        template.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        template.setBackOffPolicy(backOffPolicy);
        template.setThrowLastExceptionOnExhausted(true);
        return template;
    }
}
