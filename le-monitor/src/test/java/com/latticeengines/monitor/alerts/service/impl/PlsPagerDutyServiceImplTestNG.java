package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;

import javax.annotation.Resource;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;

public class PlsPagerDutyServiceImplTestNG extends MonitorFunctionalTestNGBase {

    @Resource(name = "plsPagerDutyService")
    private PagerDutyService pagerDutyService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((BasePagerDutyServiceImpl) pagerDutyService).useTestServiceApiKey();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", null, new BasicNameValuePair(
                "testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", null);
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", null, new BasicNameValuePair(
                "testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerDetailWithUnescapedJsonConflicts() throws ClientProtocolException, IOException,
            ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", null, new BasicNameValuePair(
                "commandLogId91", "errorCode:LEDP_00002 errorMessage:Generic system error.\n"
                        + "com.latticeengines.domain.exposed.exception.LedpException: Generic system error.\n"),
                new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

}