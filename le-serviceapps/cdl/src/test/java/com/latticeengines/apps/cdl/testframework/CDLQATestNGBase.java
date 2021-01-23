package com.latticeengines.apps.cdl.testframework;

import java.io.File;
import java.nio.file.Paths;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestFileImportService;
import com.latticeengines.testframework.exposed.service.TestJobService;
import com.latticeengines.testframework.exposed.utils.S3Utilities;
import com.latticeengines.testframework.service.impl.ContextResetTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class, ContextResetTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-qa-end2end-context.xml" })
public abstract class CDLQATestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLQATestNGBase.class);

    protected UserDocument mainUserDocument;
    protected Tenant mainTestTenant;
    protected String mainCustomerSpace;

    @Inject
    private WorkflowProxy workflowProxy;

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected TestFileImportService testFileImportService;

    @Inject
    protected TestJobService testJobService;

    @Inject
    protected DropBoxProxy dropBoxProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected CDLAttrConfigProxy cdlAttrConfigProxy;

    @Value("${qa.username}")
    protected String userName;

    @Value("${qa.password}")
    protected String password;

    @Value("${qa.maintenant}")
    protected String mainTenant;

    private static final String S3_BUCKET = "latticeengines-qa-testdata";

    private static final String S3_KEY_FOLDER = "AutomationTestData/CDLTenant/DataSet/qaend2end";

    protected String qaTestDataPath;

    @Value("${aws.qa.access.key}")
    protected String awsAccessKey;

    @Value("${aws.qa.secret.key.encrypted}")
    protected String awsSecretKey;

    @BeforeClass(alwaysRun = true)
    public void init() {
        checkBasicInfo();
        setupTestEnvironment(mainTenant);
        loginAndAttach(userName, password);
    }

    protected void checkBasicInfo() {
        Assert.assertTrue(StringUtils.isNotEmpty(userName), "Username is required");
        Assert.assertTrue(StringUtils.isNotEmpty(password), "Password is required");
        Assert.assertTrue(StringUtils.isNotEmpty(mainTenant), "Main tenant is required");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        logout();
    }

    protected void setupTestEnvironment(String existingTenant) {
        log.info("Existing tenant: " + existingTenant);
        testBed.useExistingQATenantAsMain(existingTenant);
        mainTestTenant = testBed.getMainTestTenant();
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    protected void downloadTestData() {
        S3Utilities.setS3ClientWithCredentials(awsAccessKey, awsSecretKey);
        String resourcePath = Paths.get("target").toUri().getPath();
        qaTestDataPath = resourcePath + File.separator + S3_KEY_FOLDER;
        if (!new File(qaTestDataPath).exists()) {
            log.info("Downloading s3://{}/{} to {}", S3_BUCKET, S3_KEY_FOLDER, resourcePath);
            Assert.assertTrue(S3Utilities.downloadDirectory(S3_BUCKET, S3_KEY_FOLDER, resourcePath),
                    "Failed to download qa test data!!!");
        }
    }

    protected void loginAndAttach(String userName, String password) {
        mainUserDocument = testBed.loginAndAttach(userName, password, mainTestTenant);
    }

    protected void logout(UserDocument userDoc) {
        testBed.logout(userDoc);
    }

    protected void logout() {
        if (mainUserDocument != null) {
            testBed.logout(mainUserDocument);
        }
    }

    protected SourceFile uploadDeleteCSV(String fileName, SchemaInterpretation schema, CleanupOperationType type,
            org.springframework.core.io.Resource source) {
        log.info("Upload file " + fileName + ", operation type is " + type.name() + ", Schema is " + schema.name());
        return testFileImportService.uploadDeleteFile(fileName, schema.name(), type.name(), source);
    }

    protected void cleanupTenant() {
        log.info("Starting cleanup all data in tenant");
        ApplicationId applicationId = cdlProxy.cleanupAllData(mainTenant, null, userName);
        log.info("Got app id: {} for cleanup all data", applicationId.toString());

        log.info("Waiting cleanup job...");
        JobStatus jobStatus = testJobService.waitForWorkflowStatus(mainTestTenant, applicationId.toString(), false);
        Assert.assertEquals(jobStatus, JobStatus.COMPLETED,
                String.format("The job %s cannot be completed", applicationId));

        log.info("Starting PA for cleanup...");
        testJobService.processAnalyzeRunNow(mainTestTenant);
    }
}
