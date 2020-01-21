package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.Arrays;

import javax.inject.Inject;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;
public class PagerDutyServiceTestNG extends MonitorFunctionalTestNGBase {

    @Inject
    private PagerDutyService pagerDutyService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((PagerDutyServiceImpl) this.pagerDutyService).useTestServiceApiKey();
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG",
                null, new BasicNameValuePair("testmetric", "testvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG",
                null);
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "http://PagerDutyServiceTestNG",
                null, new BasicNameValuePair("testmetric", "testvalue"), new BasicNameValuePair("anothertestmetric",
                        "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerDetailWithUnescapedJsonConflicts() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG", "localhost:8088/cluster/", null,
                new BasicNameValuePair("commandLogId91", "errorCode:LEDP_00002 errorMessage:Generic system error.\n"
                        + "com.latticeengines.domain.exposed.exception.LedpException: Generic system error.\n"),
                new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testDedupKey() throws ClientProtocolException, IOException {
        for (int i = 0; i < 3; i++) {
            String result = this.pagerDutyService.triggerEvent("PagerDutyServiceTestNG",
                    "http://PagerDutyServiceTestNG", "testDedupKey", new BasicNameValuePair("testmetric", "testvalue"+i),
                    new BasicNameValuePair("anothertestmetric", "anothertestvalue"+i));
            PagerDutyTestUtils.confirmPagerDutyIncident(result);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerWithFilter() throws ClientProtocolException, IOException {
        String result = this.pagerDutyService.triggerEvent("SocketException: Connection reset",
                "http://AlertServiceTestNG","testTriggerNoDetail");
        Assert.assertEquals(result,"filterSubjectFail");

        result = this.pagerDutyService.triggerEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                "testTriggerOneDetail", new BasicNameValuePair("c1", "Error requesting access token"));
        Assert.assertEquals(result,"filterBodyFail");

        result = this.pagerDutyService.triggerEvent("AlertServiceTestNG", "http://AlertServiceTestNG",
                "testTriggerOneDetail", new BasicNameValuePair("c1", "Test trigger one detail"));
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }

    @Test(groups = "functional", enabled = true)
    public void testTriggerLargeDescription() throws ClientProtocolException, IOException {
        char[] description_1023 = new char[1023];
        Arrays.fill(description_1023, 'a');
        String description = new String(description_1023);
        String result = this.pagerDutyService.triggerEvent(description, "http://AlertServiceTestNG",
                "description less than 1024 chars");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);

        result = this.pagerDutyService.triggerEvent(description + "bb", "http://AlertServiceTestNG",
                "description more than 1024 chars");
        PagerDutyTestUtils.confirmPagerDutyIncident(result);
    }
}
