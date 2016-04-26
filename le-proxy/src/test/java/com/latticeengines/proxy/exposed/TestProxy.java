package com.latticeengines.proxy.exposed;

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
        post("testRetry", "http://thiswillfail", null, Void.class);
    }

}
