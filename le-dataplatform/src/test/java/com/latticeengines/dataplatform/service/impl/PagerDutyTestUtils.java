package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertTrue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PagerDutyTestUtils {

    public static void confirmPagerDutyIncident(String result) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject resultObj = (JSONObject) parser.parse(result);
        assertTrue(resultObj.get("status").equals("success"));
    }

}
