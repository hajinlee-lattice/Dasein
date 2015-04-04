package com.latticeengines.pls.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.impl.AlertServiceImpl;


public class AlertServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private AlertServiceImpl alertService2;

    @BeforeClass(groups = "functional")
    public void setup() {
        alertService2.enableTestMode();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService2.triggerCriticalEvent("AlertServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"));
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService2.triggerCriticalEvent("AlertServiceTestNG");
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService2.triggerCriticalEvent("AlertServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

}
