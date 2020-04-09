package com.latticeengines.apps.dcp.testframework;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.service.impl.ContextResetTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class, ContextResetTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-dcp-context.xml" })
public abstract class DCPDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(DCPDeploymentTestNGBase.class);

    protected static final String TEST_TEMPLATE_DIR = "le-serviceapps/dcp/deployment/template";
    protected static final String TEST_DATA_DIR = "le-serviceapps/dcp/deployment/testdata";
    protected static final String TEST_TEMPLATE_NAME = "dcp-accounts-hard-coded.json";
    protected static final String TEST_TEMPLATE_VERSION = "2";
    protected static final String TEST_DATA_VERSION = "3";
    protected static final String TEST_ACCOUNT_DATA_FILE = "Account_1_900.csv";

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    protected ProjectProxy projectProxy;

    @Inject
    protected SourceProxy sourceProxy;

    @Inject
    protected UploadProxy uploadProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    @Value("${common.test.pls.url}")
    protected String deployedHostPort;

    protected Tenant mainTestTenant;
    protected String mainCustomerSpace;

    protected void setupTestEnvironment() {
        setupTestEnvironment(null);
    }

    protected void setupTestEnvironmentWithFeatureFlags(Map<String, Boolean> featureFlagMap) {
        setupTestEnvironment(null, featureFlagMap);
    }

    protected void setupTestEnvironment(String existingTenant) {
        setupTestEnvironment(existingTenant, null);
    }

    protected void setupTestEnvironment(String existingTenant, Map<String, Boolean> featureFlagMap) {
        if (!StringUtils.isEmpty(existingTenant)) {
            testBed.useExistingTenantAsMain(existingTenant);
        } else {
            if (featureFlagMap == null) {
                featureFlagMap = new HashMap<>();
            }
            // use non entity match path by default unless its overwritten explicitly
            featureFlagMap.putIfAbsent(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
            testBed.bootstrapForProduct(LatticeProduct.DCP, featureFlagMap);
        }
        mainTestTenant = testBed.getMainTestTenant();
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }

    protected void setupTestEnvironmentByFile(String jsonFileName) {
        if(!StringUtils.isEmpty(jsonFileName)) {
            testBed.bootstrapForProduct(LatticeProduct.DCP, jsonFileName);
            mainTestTenant = testBed.getMainTestTenant();
            mainCustomerSpace = mainTestTenant.getId();
            MultiTenantContext.setTenant(mainTestTenant);
            testBed.switchToSuperAdmin();
        }
    }

    protected void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        testBed.attachProtectedProxy(proxy);
        log.info("Attached the proxy " + proxy.getClass().getSimpleName() + " to GA testbed.");
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
            throw new RuntimeException("Failed to retrieve the true application id from fake id [" + applicationId + "]");
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
                if (job.getJobStatus() == JobStatus.FAILED) {
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

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }
}
