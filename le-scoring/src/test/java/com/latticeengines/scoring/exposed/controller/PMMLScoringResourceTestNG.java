package com.latticeengines.scoring.exposed.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.dmg.pmml.PMML;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class PMMLScoringResourceTestNG extends ScoringFunctionalTestNGBase {
    
    private String pmml;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        URL lrPmmlUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/exposed/controller/LogisticRegressionPMML.xml");
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

}
