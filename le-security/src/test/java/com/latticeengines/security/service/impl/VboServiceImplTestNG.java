package com.latticeengines.security.service.impl;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.security.service.VboService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class VboServiceImplTestNG extends AbstractTestNGSpringContextTests {
    private static final String TEST_SUBSCRIBER_NUMBER = "500118856";
    private static final String TEST_SUBSCRIBER_NO_DNBCONNECT = "202007225";

    @Inject
    private VboService vboService;

    @Test(groups = "functional")
    public void testGetSubscriberMeter() {
        // valid request
        JsonNode meter = vboService.getSubscriberMeter(TEST_SUBSCRIBER_NUMBER);
        Assert.assertNotNull(meter);
        Assert.assertNotNull(meter.get("current_usage"));
        Assert.assertEquals(meter.get("limit").asInt(), 100);

        // bad subscriber number
        meter = vboService.getSubscriberMeter("123456789");
        Assert.assertNull(meter);

        meter = vboService.getSubscriberMeter(null);
        Assert.assertNull(meter);

        // Subscriber missing D&B connect product
        meter = vboService.getSubscriberMeter(TEST_SUBSCRIBER_NO_DNBCONNECT);
        Assert.assertNull(meter);
    }
}
