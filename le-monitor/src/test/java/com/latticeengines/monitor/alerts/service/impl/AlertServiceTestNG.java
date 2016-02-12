package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
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
        String result = this.alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException {
        String result = this.alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException {
        String result = this.alertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"),
                new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}
