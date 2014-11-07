package com.latticeengines.dataplatform.exposed.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class AlertServiceTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    AlertServiceImpl alertService;

    @BeforeClass(groups = "functional")
    public void setup() {
        alertService.enableTestMode();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}
