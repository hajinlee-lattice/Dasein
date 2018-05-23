package com.latticeengines.pls.functionalframework;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.PdSegmentEntityMgr;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

public class PlsFunctionalTestNGBase extends PlsAbstractTestNGBase {

    protected static final String BISAP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000WjNY";
    protected static final String BISLP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000Kk28";

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

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

    protected void setupDbWithMarketoSMB(Tenant tenant) throws Exception {
        setupDbWithMarketoSMB(tenant, true, true);
    }

    protected void setupDbWithMarketoSMB(Tenant tenant, boolean createSummaries, boolean createSegments)
            throws Exception {

        ModelSummary summary1 = null;
        if (createSummaries) {
            summary1 = getDetails(tenant, "marketo");
            String[] tokens = summary1.getLookupId().split("\\|");
            tokens[1] = "Q_PLS_Modeling_" + tenant.getId();
            summary1.setLookupId(String.format("%s|%s|%s", tokens[0], tokens[1], tokens[2]));

            String modelId = summary1.getId();
            ModelSummary summary = modelSummaryEntityMgr.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryEntityMgr.deleteByModelId(summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryEntityMgr.create(summary1);
        }

        if (createSummaries && createSegments) {
            Segment segment1 = new Segment();
            segment1.setModelId(summary1.getId());
            segment1.setName("SMB");
            segment1.setPriority(1);
            segment1.setTenant(tenant);

            String modelId = segment1.getModelId();
            Segment segment = segmentEntityMgr.retrieveByModelIdForInternalOperations(modelId);
            if (segment != null) {
                setupSecurityContext(segment);
                segmentEntityMgr.deleteByModelId(segment.getModelId());
            }
            setupSecurityContext(tenant);
            segmentEntityMgr.create(segment1);
        }
    }

    protected void setupDbWithEloquaSMB(Tenant tenant) throws Exception {
        setupDbWithEloquaSMB(tenant, true, true);
    }

    protected void setupDbWithEloquaSMB(Tenant tenant, boolean createSummaries, boolean createSegments)
            throws Exception {
        ModelSummary summary2 = null;
        if (createSummaries) {
            summary2 = getDetails(tenant, "eloqua");
            Predictor s2p1 = new Predictor();
            s2p1.setApprovedUsage("Model");
            s2p1.setCategory("Construction");
            s2p1.setName("LeadSource");
            s2p1.setDisplayName("LeadSource");
            s2p1.setFundamentalType("");
            s2p1.setUncertaintyCoefficient(0.151911);
            summary2.addPredictor(s2p1);
            PredictorElement s2el1 = new PredictorElement();
            s2el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
            s2el1.setCorrelationSign(-1);
            s2el1.setCount(311L);
            s2el1.setLift(0.0);
            s2el1.setLowerInclusive(0.0);
            s2el1.setUpperExclusive(10.0);
            s2el1.setUncertaintyCoefficient(0.00313);
            s2el1.setVisible(true);
            s2p1.addPredictorElement(s2el1);

            PredictorElement s2el2 = new PredictorElement();
            s2el2.setName("7ade3995-f3da-4b83-87e6-c358ba3bdc00");
            s2el2.setCorrelationSign(1);
            s2el2.setCount(704L);
            s2el2.setLift(1.3884292375950742);
            s2el2.setLowerInclusive(10.0);
            s2el2.setUpperExclusive(1000.0);
            s2el2.setUncertaintyCoefficient(0.000499);
            s2el2.setVisible(true);
            s2p1.addPredictorElement(s2el2);

            String modelId = summary2.getId();
            ModelSummary summary = modelSummaryEntityMgr.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryEntityMgr.deleteByModelId(summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryEntityMgr.create(summary2);
        }

        if (createSummaries && createSegments) {
            Segment segment2 = new Segment();
            segment2.setModelId(summary2.getId());
            segment2.setName("SMB");
            segment2.setPriority(1);
            segment2.setTenant(tenant);

            String modelId = segment2.getModelId();
            Segment segment = segmentEntityMgr.retrieveByModelIdForInternalOperations(modelId);
            if (segment != null) {
                setupSecurityContext(segment);
                segmentEntityMgr.deleteByModelId(segment.getModelId());
            }
            setupSecurityContext(tenant);
            segmentEntityMgr.create(segment2);
        }
    }

    protected void cleanupTargetMarketDB() {
        setupSecurityContext(mainTestTenant);
        List<TargetMarket> targetMarkets = this.targetMarketEntityMgr.findAllTargetMarkets();
        for (TargetMarket targetMarket : targetMarkets) {
            if (targetMarket.getName().startsWith("TEST") || targetMarket.getIsDefault()) {
                this.targetMarketEntityMgr.deleteTargetMarketByName(targetMarket.getName());
            }
        }
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
