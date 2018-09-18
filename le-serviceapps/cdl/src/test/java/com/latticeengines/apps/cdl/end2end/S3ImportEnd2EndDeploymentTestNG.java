package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class S3ImportEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(S3ImportEnd2EndDeploymentTestNG.class);

    @BeforeClass(groups = { "manual" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
    }

    @Test(groups = "manual")
    public void runTest() {
        importData(BusinessEntity.Account, "Account_0_350.csv", "Account");
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setEntity(BusinessEntity.Account);
        config.setFeedType("Account");
        config.setS3Bucket("latticeengines-test-artifacts");
        config.setS3FilePath("/" + S3_CSV_DIR + "/" + S3_CSV_VERSION + "/Account_400_1000.csv");
        ApplicationId applicationId =  cdlProxy.submitS3ImportJob(mainCustomerSpace, config);

        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);


    }


}
