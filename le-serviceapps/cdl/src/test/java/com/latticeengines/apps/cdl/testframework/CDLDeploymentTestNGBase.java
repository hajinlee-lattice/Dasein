package com.latticeengines.apps.cdl.testframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public abstract class CDLDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLDeploymentTestNGBase.class);

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    protected WorkflowProxy workflowProxy;

    private WorkflowUtils workflowUtils;

    protected Tenant mainTestTenant;

    @PostConstruct
    private void postConstruct() {
        workflowUtils = new WorkflowUtils(workflowProxy);
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        SSLUtils.turnOffSSL();
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        testBed.switchToSuperAdmin();
    }

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        return workflowUtils.waitForWorkflowStatus(applicationId, running);
    }

}
