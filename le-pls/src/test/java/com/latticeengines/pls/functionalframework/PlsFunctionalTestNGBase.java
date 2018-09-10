package com.latticeengines.pls.functionalframework;

import java.util.List;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.PdSegmentEntityMgr;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

public class PlsFunctionalTestNGBase extends PlsDeploymentTestNGBase {

    protected static final String BISAP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000WjNY";
    protected static final String BISLP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000Kk28";

    @Autowired
    private PdSegmentEntityMgr segmentEntityMgr;

    @Autowired
    private QuotaEntityMgr quotaEntityMgr;

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Autowired
    private ProspectDiscoveryOptionEntityMgr prospectDiscoveryOptionEntityMgr;

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

    @Value("${pls.test.functional.api:http://localhost:8080/}")
    private String hostPort;

    @Autowired
    private GlobalAuthFunctionalTestBed functionalTestBed;

    protected Tenant marketoTenant;
    protected Tenant eloquaTenant;

    @PostConstruct
    private void postConstruct() {
        setTestBed(functionalTestBed);
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneGATenant();
    }

    @Override
    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    /**
     * bootstrap one tenant with random tenantId
     * 
     * @throws Exception
     */
    protected void setupTestEnvironmentWithOneGATenant() throws Exception {
        setupTestEnvironmentWithGATenants(1);
    }

    /**
     * bootstrap N tenant with random tenantId
     *
     * @throws Exception
     */
    protected void setupTestEnvironmentWithGATenants(int numTenants) throws Exception {
        testBed.bootstrap(numTenants);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    /**
     * bootstrap two tenants with random tenantIds. The first has a marketo
     * modelsummary, the second has an eloqua one. The tenants are marketoTenant
     * and eloquaTenant, the modelIds are marketoModelId and eloquaModelId.
     * 
     * @throws Exception
     */
    protected void setupMarketoEloquaTestEnvironment() throws Exception {
        testBed.bootstrap(2);
        setupDbUsingDefaultTenantIds();
        switchToSuperAdmin();
    }

    /**
     * the LedpResponseErrorHandler bound to the testbed's restTemplate. Can be
     * used to assert http errors.
     *
     * @throws Exception
     */
    protected LedpResponseErrorHandler getErrorHandler() {
        return testBed.getErrorHandler();
    }

    protected void setupDbUsingDefaultTenantIds() throws Exception {
        setupDbUsingDefaultTenantIds(true, true);
    }

    protected void setupDbUsingDefaultTenantIds(boolean useTenant1, boolean useTenant2) throws Exception {
        setupDbUsingDefaultTenantIds(useTenant1, useTenant2, true, true);
    }

    protected void setupDbUsingDefaultTenantIds(boolean useTenant1, boolean useTenant2, boolean createSummaries,
            boolean createSegments) throws Exception {
        marketoTenant = testTenants().get(0);
        eloquaTenant = testTenants().get(1);
        testBed.setMainTestTenant(eloquaTenant);
        mainTestTenant = testBed.getMainTestTenant();
        setupDbWithMarketoSMB(marketoTenant, createSummaries, createSegments);
        setupDbWithEloquaSMB(eloquaTenant, createSummaries, createSegments);
    }

    protected void cleanupQuotaDB() {
        setupSecurityContext(mainTestTenant);
        List<Quota> quotas = this.quotaEntityMgr.getAllQuotas();
        for (Quota quota : quotas) {
            if (quota.getId().startsWith("TEST")) {
                this.quotaEntityMgr.delete(quota);
            }
        }
    }

    protected void cleanupMarketoCredentialsDB() {
        List<MarketoCredential> marketoCredentials = marketoCredentialEntityMgr.findAll();
        for (MarketoCredential marketoCredential : marketoCredentials) {
            marketoCredentialEntityMgr.delete(marketoCredential);
        }
    }

    protected void cleanupProspectDiscoveryOptionDB() {
        setupSecurityContext(mainTestTenant);
        List<ProspectDiscoveryOption> prospectDiscoveryOptions = this.prospectDiscoveryOptionEntityMgr
                .findAllProspectDiscoveryOptions();
        for (ProspectDiscoveryOption option : prospectDiscoveryOptions) {
            this.prospectDiscoveryOptionEntityMgr.deleteProspectDiscoveryOption(option.getOption());
        }
    }

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }
}
