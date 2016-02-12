package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

public class PagerDutyServiceTestNG extends MonitorFunctionalTestNGBase {

    @Autowired
    private PagerDutyService pagerDutyService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((PagerDutyServiceImpl) this.pagerDutyService).useTestServiceApiKey();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric",
                        "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerDetailWithUnescapedJsonConflicts() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "localhost:8088/cluster/",
                new BasicNameValuePair("commandLogId91", "errorCode:LEDP_00002 errorMessage:Generic system error.\n"
                        + "com.latticeengines.domain.exposed.exception.LedpException: Generic system error.\n"),
                new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}
