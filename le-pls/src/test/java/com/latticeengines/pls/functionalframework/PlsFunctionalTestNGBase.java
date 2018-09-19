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
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

public class PlsFunctionalTestNGBase extends PlsAbstractTestNGBase {

    @Autowired
    private QuotaEntityMgr quotaEntityMgr;

    @Autowired
    private ProspectDiscoveryOptionEntityMgr prospectDiscoveryOptionEntityMgr;

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

    @Value("${pls.test.functional.api}")
    private String hostPort;

    @Autowired
    private GlobalAuthFunctionalTestBed functionalTestBed;

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
