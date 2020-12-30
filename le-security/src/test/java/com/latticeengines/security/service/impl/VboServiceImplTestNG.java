package com.latticeengines.security.service.impl;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
import com.latticeengines.security.service.VboService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class VboServiceImplTestNG extends AbstractTestNGSpringContextTests {
    private static final String TEST_SUBSCRIBER_NUMBER = "500118856";
    private static final String TEST_SUBSCRIBER_EMAIL = "testDCP1@outlook.com";
    private static final String TEST_SUBSCRIBER_NO_DNBCONNECT = "202007225";
    private static final Date CURRENT_DATE = new Date();

    @Inject
    private VboService vboService;

    @Test(groups = "functional")
    public void testGetSubscriberMeter() {
        // valid request
        JsonNode meter = vboService.getSubscriberMeter(TEST_SUBSCRIBER_NUMBER);
        Assert.assertNotNull(meter);
        Assert.assertTrue(meter.has("current_usage"));
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

    @Test(groups = "functional")
    public void testSendUserUsageEvent() {
        // retrieve meter value
        JsonNode meter = vboService.getSubscriberMeter(TEST_SUBSCRIBER_NUMBER);
        Assert.assertNotNull(meter);
        Assert.assertTrue(meter.has("current_usage"));
        int currentUsage = meter.get("current_usage").asInt(0);

        // valid STCT (increment) request
        VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
        populateUsageEvent(usageEvent);
        try {
            vboService.sendUserUsageEvent(usageEvent);
            Thread.sleep(3000);
            int updatedUsage = vboService.getSubscriberMeter(TEST_SUBSCRIBER_NUMBER).get("current_usage").asInt();
            Assert.assertEquals(updatedUsage, currentUsage + 1);
        } catch (Exception e) {
            Assert.fail("Failed to increment seat count.");
        }

        // bad STCT request
        usageEvent.setSubscriberID(null);
        try {
            vboService.sendUserUsageEvent(usageEvent);
            Assert.fail("Usage event request succeeded, but should have failed.");
        } catch (Exception e){
            System.out.println("Usage event request failed as expected.");
        }
    }

    private void populateUsageEvent(VboUserSeatUsageEvent usageEvent) {
        usageEvent.setEmailAddress(TEST_SUBSCRIBER_EMAIL);
        usageEvent.setSubscriberID(TEST_SUBSCRIBER_NUMBER);
        usageEvent.setFeatureURI(VboUserSeatUsageEvent.FeatureURI.STCT);
        usageEvent.setPOAEID("1");
        usageEvent.setLUID(null);
        usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        usageEvent.setSubjectCountry("US");
        usageEvent.setSubscriberCountry("US");
        usageEvent.setContractTermStartDate(CURRENT_DATE);
        usageEvent.setContractTermEndDate(DateUtils.addHours(CURRENT_DATE, 1));
    }
}
