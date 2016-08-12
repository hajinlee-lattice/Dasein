package com.latticeengines.proxy.exposed;

import com.latticeengines.common.exposed.util.PropertyUtils;
import org.springframework.stereotype.Component;
import org.testng.Assert;

@Component("testProxy")
public class TestProxy extends BaseRestApiProxy implements TestInterface {

    public TestProxy() {
        super(PropertyUtils.getProperty("proxy.test.rest.endpoint.hostport"), "foo/{bar}", "baz");
    }

    @Override
    public void testUrlExpansion() {
        String url = constructUrl("value?customer={customer}", "test");
        Assert.assertEquals(url, getHostport() + "/foo/baz/value?customer=test");

        String home = System.getenv("HOME");
        Assert.assertTrue(url.startsWith("http://" + home));
    }

    @Override
    public void testRetry() {
        post("testRetry", "http://thiswillfail", null, Void.class);
    }

}
