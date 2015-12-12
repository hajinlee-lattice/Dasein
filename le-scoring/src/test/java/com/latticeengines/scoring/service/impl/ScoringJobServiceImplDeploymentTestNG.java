package com.latticeengines.scoring.service.impl;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;

public class ScoringJobServiceImplDeploymentTestNG extends ScoringJobServiceImplTestNG {

    @Value("${scoring.test.microservice.url}")
    private String scoringMicroserviceUrl;

    protected static String customer = "Mulesoft_Relaunch_Deployment";

    private RestTemplate restTemplate = new RestTemplate();

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        samplePath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/samples/";
        metadataPath = customerBaseDir + "/" + tenant + "/data/EventMetadata/";
        modelingModelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
    }

    @Test(groups = "deployment")
    public void modelScoreAndCompare() throws Exception {
        super.modelScoreAndCompare();
    }

    @Override
    protected void scoring() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn("ModelingID");
        AppSubmission submission = restTemplate.postForObject(scoringMicroserviceUrl + "/scoringjobs", scoringConfig,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = getApplicationId(submission.getApplicationIds().get(0));
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
    }
}
