package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class S3ImportEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final String S3_BUCKET = "latticeengines-test-artifacts";
    private static final String ENTITY_ACCOUNT = "Account";
    private static final String FEED_TYPE_SUFFIX = "Schema";
    @Value("${aws.region}")
    private String awsRegion;

    @BeforeClass(groups = { "manual", "deployment" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
    }

    @Test(groups = "manual")
    public void runTest() {
        importData(BusinessEntity.Account, "Account_401_500.csv");
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setFeedType(ENTITY_ACCOUNT + FEED_TYPE_SUFFIX);
        config.setS3Bucket(S3_BUCKET);
        config.setS3FilePath("/" + S3_CSV_DIR + "/" + S3_CSV_VERSION + "/Account_901_1000.csv");
        ApplicationId applicationId =  cdlProxy.submitS3ImportJob(mainCustomerSpace, config);

        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);


    }

    @Test(groups = "deployment")
    public void testS3Import() {
        S3FileToHdfsConfiguration importConfig = null;
        catchException(importConfig);
        importConfig = new S3FileToHdfsConfiguration();
        catchException(importConfig);
        importConfig.setFeedType(ENTITY_ACCOUNT + FEED_TYPE_SUFFIX);
        catchException(importConfig);
        importConfig.setFilePath("");
        importConfig.setS3Bucket(S3_BUCKET);
        importConfig.setS3FilePath("/" + S3_CSV_DIR + "/" + S3_CSV_VERSION + "/Account_901_1000.csv");
        catchException(importConfig);
    }

    private void catchException(S3FileToHdfsConfiguration importConfig) {
        Assert.assertThrows(RuntimeException.class, () -> cdlProxy.submitS3ImportJob(mainCustomerSpace, importConfig));
    }




}
