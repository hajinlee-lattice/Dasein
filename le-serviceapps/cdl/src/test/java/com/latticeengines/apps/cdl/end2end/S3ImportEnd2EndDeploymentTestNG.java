package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.cdl.DropBoxAccessMode.LatticeUser;

import java.io.InputStream;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class S3ImportEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(S3ImportEnd2EndDeploymentTestNG.class);

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxService dropBoxService;

    @Value("${aws.region}")
    private String awsRegion;

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
        config.setFeedType("Account");
        config.setS3Bucket("latticeengines-test-artifacts");
        config.setS3FilePath("/" + S3_CSV_DIR + "/" + S3_CSV_VERSION + "/Account_400_1000.csv");
        ApplicationId applicationId =  cdlProxy.submitS3ImportJob(mainCustomerSpace, config);

        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);


    }

    @Test(groups = "manual")
    public void runTestWithSQS() {
        importData(BusinessEntity.Account, "Account_0_350.csv", "Account");

        log.info(String.format("Dropbox bucket %s, prefix %s", dropBoxService.getDropBoxBucket(), dropBoxService.getDropBoxPrefix()));

        System.out.println("Test Finish!");

    }



}
