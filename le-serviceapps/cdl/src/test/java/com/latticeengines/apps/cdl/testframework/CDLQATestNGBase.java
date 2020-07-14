package com.latticeengines.apps.cdl.testframework;

import java.util.Collections;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
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
        Preconditions.checkState(StringUtils.isNotEmpty(userName), "Username is required");
        Preconditions.checkState(StringUtils.isNotEmpty(password), "Password is required");
        Preconditions.checkState(StringUtils.isNotEmpty(mainTenant), "Main tenant is required");
    }

    @AfterClass(groups = { "qaend2end" })
    public void tearDown() {
        logout();
    }

    // @Override
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

    protected String waitForTrueApplicationId(String applicationId) {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        if (StringUtils.isBlank(applicationId)) {
            throw new IllegalArgumentException("Must provide a valid fake application id");
        }
        if (!ApplicationIdUtils.isFakeApplicationId(applicationId)) {
            return applicationId;
        }
        RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate( //
                100, 1000, 2, 3000, //
                false, Collections.emptyMap());
        try {
            return retry.execute(ctx -> {
                if (ctx.getLastThrowable() != null) {
                    log.error("Failed to retrieve Job using application id " + applicationId, ctx.getLastThrowable());
                }
                Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId, customerSpace);
                String newId = job.getApplicationId();
                if (!ApplicationIdUtils.isFakeApplicationId(newId)) {
                    return newId;
                } else {
                    throw new IllegalStateException("Still showing fake id " + newId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to retrieve the true application id from fake id [" + applicationId + "]");
        }
    }

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        String trueAppId = waitForTrueApplicationId(applicationId);
        if (!trueAppId.equals(applicationId)) {
            log.info("Convert fake app id " + applicationId + " to true app id " + trueAppId);
        }
        int retryOnException = 4;
        Job job;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(trueAppId,
                        CustomerSpace.parse(mainTestTenant.getId()).toString());
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                if (job.getJobStatus() == JobStatus.FAILED || job.getJobStatus() == JobStatus.PENDING_RETRY) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
