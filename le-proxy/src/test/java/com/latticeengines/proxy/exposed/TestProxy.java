package com.latticeengines.proxy.exposed;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;

@Component("testProxy")
public class TestProxy extends BaseRestApiProxy implements TestInterface {

    public TestProxy() {
        super("foo/{bar}", "baz");
    }

    @Override
    public void testUrlExpansion() {
        String url = constructUrl("value?customer={customer}", "test");
        Assert.assertEquals(url, getMicroserviceHostPort() + "/foo/baz/value?customer=test");
    }

    @Override
    public void testRetry() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);
        backOffPolicy.setMultiplier(2);
        retry.setBackOffPolicy(backOffPolicy);
        post("testRetry", "http://thiswillfail", null, Void.class, retry);
    }

}
