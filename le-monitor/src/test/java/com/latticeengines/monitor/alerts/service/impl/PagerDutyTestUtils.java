package com.latticeengines.monitor.alerts.service.impl;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class PagerDutyTestUtils {

    static void confirmPagerDutyIncident(String result) {
        try {
            JsonNode resultObj = new ObjectMapper().readTree(result);
            assertEquals(resultObj.get("status").asText(), "success");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
