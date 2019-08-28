package com.latticeengines.workflowapi.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
public class WorkflowApiDeploymentTestNGBase extends WorkflowApiFunctionalTestNGBase {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Autowired
    private TenantService tenantService;

    @Inject
    private VersionManager versionManager;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
    protected Tenant mainTestTenant;
    protected CustomerSpace mainTestCustomerSpace;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @PostConstruct
    public void postConstruct() {
        restTemplate = testBed.getRestTemplate();
        magicRestTemplate = testBed.getMagicRestTemplate();
    }

    protected void setupTestEnvironment(LatticeProduct product) throws Exception {
        setupTestTenant(product);
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        setupYarnPlatform();
    }

    /**
     * Child class can override this, if it needs different environment
     */
    protected void setupTestTenant(LatticeProduct product) {
        setupTestEnvironmentWithOneTenantForProduct(product);
        Tenant tenantWithPid = tenantService.findByTenantId(mainTestTenant.getId());
        mainTestTenant = tenantWithPid;
        MultiTenantContext.setTenant(tenantWithPid);
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)  {
        SSLUtils.turnOffSSLNameVerification();
        testBed.bootstrapForProduct(product);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        testBed.switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
        assertNotNull(MultiTenantContext.getTenant());
    }

    protected void runWorkflow(WorkflowConfiguration workflowConfig) throws Exception {
        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS, 1000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
