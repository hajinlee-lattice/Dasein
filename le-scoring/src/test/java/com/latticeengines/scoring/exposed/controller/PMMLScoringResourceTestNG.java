package com.latticeengines.scoring.exposed.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.dmg.pmml.PMML;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.exposed.domain.ScoringResponse;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class PMMLScoringResourceTestNG extends ScoringFunctionalTestNGBase {
    
    private String pmml;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        URL lrPmmlUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/LogisticRegressionPMML.xml");
        pmml = FileUtils.readFileToString(new File(lrPmmlUrl.getFile()));
    }

    @Test(groups = "functional")
    public void deploy() throws Exception {
        HttpEntity<String> stringEntity = new HttpEntity<String>(pmml);
        ResponseEntity<String> result = restTemplate.exchange("http://localhost:8080/rest/pmml/abcde", HttpMethod.PUT,
                stringEntity, String.class, new HashMap<String, Object>());
        assertEquals(result.getBody(), "Model abcde deployed successfully.");
        assertEquals(result.getStatusCode(), HttpStatus.OK);
    }

    @Test(groups = "functional", dependsOnMethods = { "deploy" })
    public void get() throws Exception {
        ResponseEntity<PMML> response = restTemplate.getForEntity("http://localhost:8080/rest/pmml/abcde", PMML.class);
        assertNotNull(response.getBody());
    }

    @Test(groups = "functional", dependsOnMethods = { "get" })
    public void score() throws Exception {
        ScoringRequest request = new ScoringRequest();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("age", 30f);
        params.put("salary", 65000f);
        params.put("car_location", "street");
        request.setArguments(params);
        ScoringResponse response = restTemplate.postForObject("http://localhost:8080/rest/pmml/abcde", request, ScoringResponse.class, new HashMap<String, Object>());
        Double value = (Double) response.getResult().get("number_of_claims");
        assertNotNull(value);
        assertEquals(value, Double.valueOf(1320.4));
    }
    
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        restTemplate.delete("http://localhost:8080/rest/pmml/abcde", new HashMap<String, Object>());
        ResponseEntity<PMML> response = restTemplate.getForEntity("http://localhost:8080/rest/pmml/abcde", PMML.class);
        assertNull(response.getBody());
    }
    
}
