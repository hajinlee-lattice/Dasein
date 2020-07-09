package com.latticeengines.apps.cdl.testframework;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.google.common.base.Preconditions;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public abstract class CDLQATestNGBase extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLQATestNGBase.class);
    protected UserDocument mainUserDocument;

    @Inject
    protected CDLProxy cdlProxy;

    @Value("${qa.username}")
    protected String userName;

    @Value("${qa.password}")
    protected String password;

    @Value("${qa.maintenant}")
    protected String mainTenant;

    @BeforeClass
    public void init() {
        checkBasicInfo();
        setupTestEnvironment(mainTenant);
        loginAndAttach(userName, password);
    }

    protected void checkBasicInfo() {
        Preconditions.checkState(StringUtils.isNotEmpty(userName), "Username is required");
        Preconditions.checkState(StringUtils.isNotEmpty(password), "Password is required");
        Preconditions.checkState(StringUtils.isNotEmpty(mainTenant), "Main tenant is required");
    }

    @AfterClass
    public void tearDown() {
        logout();
    }

    @Override
    protected void setupTestEnvironment(String existingTenant) {
        testBed.useExistingQATenantAsMain(existingTenant);
        mainTestTenant = testBed.getMainTestTenant();
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
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

    protected void processAnalyzeRunNow() {
        processAnalyze(true, null);
    }

    protected void processAnalyze(boolean runNow, ProcessAnalyzeRequest processAnalyzeRequest) {
        log.info("Start processing and analyzing on tenant {}", mainTenant);
        ApplicationId applicationId = cdlProxy.scheduleProcessAnalyze(mainTenant, runNow, processAnalyzeRequest);
        log.info("Got applicationId={}", applicationId);

        log.info("Waiting job...");
        JobStatus jobStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(jobStatus, JobStatus.COMPLETED,
                String.format("The PA job %s cannot be completed", applicationId));

        log.info("The PA job {} is completed", applicationId);
    }

    protected void cleanupTenant() {
        log.info("Starting cleanup all data in tenant");
        ApplicationId applicationId = cdlProxy.cleanupAllData(mainTenant, null, userName);
        log.info("Got app id: {} for cleanup all data", applicationId.toString());

        log.info("Waiting cleanup job...");
        JobStatus jobStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(jobStatus, JobStatus.COMPLETED,
                String.format("The job %s cannot be completed", applicationId));

        log.info("Starting PA for cleanup...");
        processAnalyzeRunNow();
    }
}
