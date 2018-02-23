package com.latticeengines.proxy.exposed;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.http.HttpMethod;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.PropertyUtils;

@Component("testProxy")
public class TestProxy extends BaseRestApiProxy {

    public TestProxy() {
        super(PropertyUtils.getProperty("proxy.test.rest.endpoint.hostport"), "foo/{bar}", "baz");
    }

    public void testUrlExpansion() {
        String url = constructUrl("value?customer={customer}", "test");
        Assert.assertEquals(url, getHostport() + "/foo/baz/value?customer=test");
    }

    public String testRetry() {
        return get("testRetry", constructUrl("/retry"), String.class);
    }

    public String getWithCounter(String endpoint, AtomicInteger counter) {
        String url = constructUrl("/" + endpoint);
        return getWithCounter("get " + endpoint,  url, String.class, counter);
    }

    private  <T> T getWithCounter(final String method, final String url, final Class<T> returnValueClazz, final AtomicInteger counter) {
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate(method, verb, url, false, null);
        return retry.execute(context -> {
            counter.incrementAndGet();
            return exchange(url, verb, null, returnValueClazz, false, false).getBody();
        });
    }
    
    

}
