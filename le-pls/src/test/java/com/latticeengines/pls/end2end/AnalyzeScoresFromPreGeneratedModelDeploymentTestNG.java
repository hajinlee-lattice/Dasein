package com.latticeengines.pls.end2end;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;

@Component
public class AnalyzeScoresFromPreGeneratedModelDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final String TENANT_ID = "DevelopTestPLSTenant2.DevelopTestPLSTenant2.Production";
    private static final String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";

    @Value("${pls.scoringapi.rest.endpoint.hostport}")
    private String scoringApiHostPort;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreCorrectnessService scoreCorrectnessService;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void useLocalScoredTextAndCompareScores() throws InterruptedException, IOException {
        String modelId = "ms__07be545c-ad99-4ba3-b56d-7c98487fa0d5-SelfServ";
        String pathToModelInputCsv = RESOURCE_BASE + "/" + fileName;
        scoreCorrectnessService.analyzeScores(TENANT_ID, pathToModelInputCsv, modelId);
    }

}
