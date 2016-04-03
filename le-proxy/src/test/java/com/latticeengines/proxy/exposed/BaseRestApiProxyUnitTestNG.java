package com.latticeengines.proxy.exposed;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class BaseRestApiProxyUnitTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private TestProxy testProxy;

    @Test(groups = "unit")
    public void testUrlExpansion() {
        testProxy.testUrlExpansion();
    }
}
