package com.latticeengines.pls.service.impl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.impl.PagerDutyServiceImpl;


public class PagerDutyServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private PagerDutyServiceImpl pagerDutyService2;

    @BeforeClass(groups = "functional")
    public void setup() {
        pagerDutyService2.useTestServiceApiKey();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService2.triggerEvent("PagerDutyServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"));
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService2.triggerEvent("PagerDutyServiceTestNG");
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService2.triggerEvent("PagerDutyServiceTestNG",
                new BasicNameValuePair("testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric",
                        "anothertestvalue"));
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerDetailWithUnescapedJsonConflicts() throws ClientProtocolException, IOException,
            ParseException {
        String result = pagerDutyService2.triggerEvent("PagerDutyServiceTestNG",
                new BasicNameValuePair("commandLogId91", "errorCode:LEDP_00002 errorMessage:Generic system error.\n"
                        + "com.latticeengines.domain.exposed.exception.LedpException: Generic system error.\n"),
                new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyImplTestUtils.confirmPagerDutyIncident(result);
    }

}