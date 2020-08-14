package com.latticeengines.aws.dynamo;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public final class DynamoRetryUtils {

    protected DynamoRetryUtils() {
        throw new UnsupportedOperationException();
    }

    /*
     * Retry templates that know which dynamo exception can/cannot retry
     */

    /*
     * with exponential backoff
     */
    public static RetryTemplate getExponentialBackOffTemplate(int maxAttempts, long initialWaitMsec, long maxWaitMSec,
            double multiplier) {
        RetryTemplate template = getSimpleRetryTemplate(maxAttempts);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialRandomBackOffPolicy();
        // set backoff parameters
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        backOffPolicy.setMaxInterval(maxWaitMSec);
        template.setBackOffPolicy(backOffPolicy);
        return template;
    }

    /*
     * just retry specified times
     */
    public static RetryTemplate getSimpleRetryTemplate(int maxAttempts) {
        RetryTemplate template = new RetryTemplate();
        RetryPolicy retryPolicy = new DynamoRetryPolicy(new SimpleRetryPolicy(maxAttempts));
        template.setRetryPolicy(retryPolicy);
        template.setThrowLastExceptionOnExhausted(true);
        return template;
    }
}
