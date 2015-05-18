package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public class ModelingAlertServiceTestNG extends MonitorFunctionalTestNGBase {

    @Autowired
    private AlertService modelingAlertService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((BaseAlertServiceImpl) modelingAlertService).enableTestMode();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = modelingAlertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = modelingAlertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = modelingAlertService.triggerCriticalEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric",
                        "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}
