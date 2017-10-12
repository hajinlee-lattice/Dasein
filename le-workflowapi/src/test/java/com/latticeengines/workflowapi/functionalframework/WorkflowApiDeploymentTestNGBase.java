package com.latticeengines.workflowapi.functionalframework;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
public class WorkflowApiDeploymentTestNGBase extends WorkflowApiFunctionalTestNGBase {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Autowired
    private TenantService tenantService;

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

    @Override
    @BeforeClass(groups = { "deployment", "workflow" })
    public void setup() throws Exception {
        setupTestTenant();
        if (softwareLibraryService != null) {
            softwareLibraryService.setStackName(stackName);
        }
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        setupYarnPlatform();
    }

    /**
     * Child class can override this, if it needs different environment
     */
    protected void setupTestTenant() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        Tenant tenantWithPid = tenantService.findByTenantId(mainTestTenant.getId());
        mainTestTenant = tenantWithPid;
        MultiTenantContext.setTenant(tenantWithPid);
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        SSLUtils.turnOffSSLNameVerification();
        testBed.bootstrapForProduct(product);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        testBed.switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
        assertNotNull(MultiTenantContext.getTenant());
    }

}
