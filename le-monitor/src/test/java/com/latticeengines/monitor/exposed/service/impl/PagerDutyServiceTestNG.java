package com.latticeengines.monitor.exposed.service.impl;

import java.io.IOException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.domain.exposed.exception.LedpException;

public class PagerDutyServiceTestNG { //extends MonitorFunctionalTestNGBase {

    @Autowired
    PagerDutyServiceImpl pagerDutyService;

    @BeforeClass(groups = "functional")
    public void setup() {
        pagerDutyService.useTestServiceApiKey();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
    	boolean flag = true;
    	try {
	        pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG", new BasicNameValuePair("testmetric",
	                "testvalue"));
    	} catch (LedpException e) {
    		flag = false;
    	}
        PagerDutyTestUtils.confirmPagerDutyIncident(flag);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
    	boolean flag = true;
    	try {
	        pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG");
    	} catch (LedpException e) {
    		flag = false;
    	}
        PagerDutyTestUtils.confirmPagerDutyIncident(flag);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
    	boolean flag = true;
    	try {
	        pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG", new BasicNameValuePair("testmetric",
	                "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
    	} catch (LedpException e) {
    		flag = false;
    	}
        PagerDutyTestUtils.confirmPagerDutyIncident(flag);
    }
}

