package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.PagerDutyService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class PagerDutyServiceTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    PagerDutyService pagerDutyService;

    @Test(groups = "functional", enabled = false)
    public void testTriggerOneDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"));
        confirmResult(result);
    }

    @Test(groups = "functional", enabled = false)
    public void testTriggerNoDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG");
        confirmResult(result);
    }

    @Test(groups = "functional", enabled = false)
    public void testTriggerMultipleDetail() throws ClientProtocolException, IOException, ParseException {
        String result = pagerDutyService.triggerEvent("PagerDutyServiceTestNG", new BasicNameValuePair("testmetric",
                "testvalue"), new BasicNameValuePair("anothertestmetric", "anothertestvalue"));
        confirmResult(result);
    }

    private void confirmResult(String result) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject resultObj = (JSONObject) parser.parse(result);
        assertTrue(resultObj.get("status").equals("success"));
    }

}
