package com.latticeengines.scoring.service.impl;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.runtime.mapreduce.ScoringComparisonAgainstModelingTestNG;
import com.latticeengines.scoring.service.ScoringJobService;

public class ScoringJobServiceImplTestNG extends ScoringComparisonAgainstModelingTestNG {

    protected static String customer = "Mulesoft_Relaunch_JobService";

    @Autowired
    private ScoringJobService scoringJobService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        samplePath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/samples/";
        metadataPath = customerBaseDir + "/" + tenant + "/data/EventMetadata/";
        modelingModelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
    }

    @Override
    protected void prepareDataForScoring() throws Exception {
        uuid = getUuid();
        containerId = getContainerId();
        System.out.println("uuid is " + uuid);
    }

    @Override
    protected void score() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn("ModelingID");
        ApplicationId appId = scoringJobService.score(scoringConfig);
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
