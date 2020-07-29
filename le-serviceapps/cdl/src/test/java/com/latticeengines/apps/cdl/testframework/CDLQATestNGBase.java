package com.latticeengines.apps.cdl.testframework;

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
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestFileImportService;
import com.latticeengines.testframework.exposed.service.TestJobService;
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

    @Value("${qa.username}")
    protected String userName;

    @Value("${qa.password}")
    protected String password;

    @Value("${qa.maintenant}")
    protected String mainTenant;

    @BeforeClass(groups = { "qaend2end" })
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

    @AfterClass(groups = { "qaend2end" })
    public void tearDown() {
        logout();
    }

    protected void setupTestEnvironment(String existingTenant) {
        System.out.println("Existing tenant: " + existingTenant);
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
