package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public class AlertServiceTestNG extends MonitorFunctionalTestNGBase {

    @Autowired
    private AlertService alertService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((AlertServiceImpl) this.alertService).enableTestMode();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException {
        alertService.triggerCriticalEvent("testTriggerOneDetail", "http://AlertServiceTestNG",
                "testTriggerOneDetail", new BasicNameValuePair("testmetric", "testvalue"));
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException {
        alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG", "testTriggerNoDetail");
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException {
        alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                "testTriggerMultipleDetail", new BasicNameValuePair("testmetric", "testvalue"), new BasicNameValuePair(
                        "anothertestmetric", "anothertestvalue"));
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerWithFilter() throws ClientProtocolException, IOException {
        String result = alertService.triggerCriticalEvent("SocketException: Connection reset",
                "http://AlertServiceTestNG","testTriggerNoDetail");
        Assert.assertEquals(result,"filterSubjectFail");

        result = alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                "testTriggerOneDetail", new BasicNameValuePair("c1", "Error requesting access token"));
        Assert.assertEquals(result,"filterBodyFail");

        result = alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                "testTriggerOneDetail", new BasicNameValuePair("c1", "Test trigger one detail"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }
}
