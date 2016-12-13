package com.latticeengines.pls.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class ScoringApiConsoleResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testWithDomain() throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = getRestAPIHostPort() + "/pls/scores/apiconsole/record/debug";

        ScoreRequest scoreRequest = new ScoreRequest();
        populateScoreRequest(scoreRequest, false);
        DebugScoreResponse resp = restTemplate.postForObject(url, scoreRequest, DebugScoreResponse.class);
        assertNotNull(resp);
        assertNotNull(resp.getEnrichmentAttributeValues());
        assertTrue(resp.getEnrichmentAttributeValues().size() > 16000);

        int nonNullEnrichmentValues = 0;
        for (String key : resp.getEnrichmentAttributeValues().keySet()) {
            if (resp.getEnrichmentAttributeValues().get(key) != null) {
                nonNullEnrichmentValues++;
            }
        }

        assertTrue(nonNullEnrichmentValues > 5000, "Actual : " + nonNullEnrichmentValues);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testWithDUNS() throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = getRestAPIHostPort() + "/pls/scores/apiconsole/record/debug";

        ScoreRequest scoreRequest = new ScoreRequest();
        populateScoreRequest(scoreRequest, true);
        DebugScoreResponse resp = restTemplate.postForObject(url, scoreRequest, DebugScoreResponse.class);
        assertNotNull(resp);
        assertNotNull(resp.getEnrichmentAttributeValues());
        assertTrue(resp.getEnrichmentAttributeValues().size() > 16000);

        int nonNullEnrichmentValues = 0;
        for (String key : resp.getEnrichmentAttributeValues().keySet()) {
            if (resp.getEnrichmentAttributeValues().get(key) != null) {
                nonNullEnrichmentValues++;
            }
        }

        assertTrue(nonNullEnrichmentValues > 5000, "Actual : " + nonNullEnrichmentValues);
    }

    private void populateScoreRequest(ScoreRequest scoreRequest, boolean withDuns) {
        Map<String, Object> record = new HashMap<>();
        if (withDuns) {
            record.put("DUNS", "196337864");
        } else {
            record.put("Email", "a@microsoft.com");
        }
        scoreRequest.setPerformEnrichment(true);
        scoreRequest.setRecordId(UUID.randomUUID().toString());
        scoreRequest.setRecord(record);
    }

}
