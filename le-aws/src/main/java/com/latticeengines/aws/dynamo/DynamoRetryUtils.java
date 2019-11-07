package com.latticeengines.aws.dynamo;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public class DynamoRetryUtils {

    /*
     * Retry template that know which dynamo exception can/cannot retry
     */
    public static RetryTemplate getExponentialBackOffTemplate(int maxAttempts, long initialWaitMsec, long maxWaitMSec,
            double multiplier) {
        RetryTemplate template = new RetryTemplate();
        RetryPolicy retryPolicy = new DynamoRetryPolicy(new SimpleRetryPolicy(maxAttempts));
        template.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        // set backoff parameters
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        backOffPolicy.setMaxInterval(maxWaitMSec);

        template.setBackOffPolicy(backOffPolicy);
        template.setThrowLastExceptionOnExhausted(true);
        return template;
    }
}
