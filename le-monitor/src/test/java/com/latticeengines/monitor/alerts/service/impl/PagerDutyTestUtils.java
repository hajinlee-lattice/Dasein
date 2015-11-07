package com.latticeengines.monitor.alerts.service.impl;

import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PagerDutyTestUtils {

    public static void confirmPagerDutyIncident(String result) {
        try {
            JsonNode resultObj = new ObjectMapper().readTree(result);
            assertTrue(resultObj.get("status").asText().equals("success"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
