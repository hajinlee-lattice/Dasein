package com.latticeengines.scoring.service.impl;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringJobService;

public class ScoringContainerTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringJobService scoringJobService;

    private String customer = "test_customer";

    private String tenant;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    private String path;

    private String dataPath;

    private String scorePath;

    private String uuid = "ac52c4b7-b856-456d-b3a6-6dc758139eaf";

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = path + "/scoring/data";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
        HdfsUtils.rmdir(yarnConfiguration, scorePath);
    }

    @Test(groups = "functional", enabled = false)
    protected void scoring() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn("LeadID");
        ApplicationId appId = scoringJobService.score(scoringConfig);
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
    }

}
