package com.latticeengines.scoring.service.impl;

import java.util.Collections;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.proxy.exposed.scoring.ScoringProxy;

public class ScoringJobServiceImplDeploymentTestNG extends ScoringJobServiceImplTestNG {

    @Value("${common.test.microservice.url}")
    private String microserviceUrl;

    @Autowired
    private ScoringProxy scoringProxy;

    protected static String customer = "Mulesoft_Relaunch_Deployment";

    @Override
    @BeforeClass(groups = "deployment", enabled = false)
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        samplePath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/samples/";
        metadataPath = customerBaseDir + "/" + tenant + "/data/EventMetadata/";
        modelingModelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
    }

    @Test(groups = "deployment", enabled = false)
    public void modelScoreAndCompare() throws Exception {
        super.modelScoreAndCompare();
    }

    @Override
    protected void score() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Collections.singletonList("ms__" + uuid + "-PLS_model"));
        scoringConfig.setUniqueKeyColumn("ModelingID");
        AppSubmission submission = scoringProxy.createScoringJob(scoringConfig);
        ApplicationId appId = getApplicationId(submission.getApplicationIds().get(0));
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
    }
}
