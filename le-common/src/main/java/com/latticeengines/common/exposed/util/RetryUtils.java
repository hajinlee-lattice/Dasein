package com.latticeengines.common.exposed.util;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RetryUtils {

    private static final long INITIAL_WAIT_INTERVAL = 100L;

    public static long getExponentialWaitTime(int retryCount) {
        return retryCount == 0 ? 0 : ((long) Math.pow(2, retryCount) * INITIAL_WAIT_INTERVAL);
    }

    public static RetryTemplate getExponentialBackoffRetryTemplate(
            int maxAttempts, long initialWaitMsec, double multiplier, List<Class<? extends Throwable>> retryExceptions) {
        RetryTemplate template = new RetryTemplate();
        Map<Class<? extends Throwable>, Boolean> retryExceptionMap = retryExceptions
                .stream().collect(Collectors.toMap(e -> e, e -> true));
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
