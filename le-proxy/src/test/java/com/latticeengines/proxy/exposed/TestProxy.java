package com.latticeengines.proxy.exposed;

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

    public String testDirectlyFail() {
        return get("testDirectlyFail", constructUrl("/runtime"), String.class);
    }

    public String testRetryAndFail() {
        return get("testRetryAndFail", constructUrl("/timeout"), String.class);
    }
    
    

}
