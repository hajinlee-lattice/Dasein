package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private SelfServeModelingToScoringEndToEndDeploymentTestNG selfServiceModeling;

    private String modelId;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        modelId = selfServiceModeling.prepareModel();
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void testScoreTrainingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId));
        String response = restTemplate.postForObject(
                String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId), //
                null, String.class);
        assertNotNull(response);
    }
}
