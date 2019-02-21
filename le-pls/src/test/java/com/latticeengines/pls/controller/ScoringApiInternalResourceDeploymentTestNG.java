package com.latticeengines.pls.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class ScoringApiInternalResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    private String getScoringApiInternalUrl() {
        return getRestAPIHostPort() + "/pls/scoringapi-internal";
    }

    @Test(groups = "deployment")
    public void testWithDomain() {
        String url = getScoringApiInternalUrl() + "/record/apiconsole/debug";

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

    @Test(groups = "deployment")
    public void testWithDUNS() {
        String url = getScoringApiInternalUrl() + "/record/apiconsole/debug";

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
