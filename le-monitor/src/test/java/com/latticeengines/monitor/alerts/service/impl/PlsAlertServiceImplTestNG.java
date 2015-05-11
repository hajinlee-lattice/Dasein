package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import javax.annotation.Resource;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.alerts.service.impl.BaseAlertServiceImpl;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public class PlsAlertServiceImplTestNG extends MonitorFunctionalTestNGBase {

    @Resource(name = "plsAlertService")
    private AlertService alertService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((BaseAlertServiceImpl) alertService).enableTestMode();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", null, new BasicNameValuePair(
                "testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", null);
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = alertService.triggerCriticalEvent("AlertServiceTestNG", null, new BasicNameValuePair(
                "testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}
