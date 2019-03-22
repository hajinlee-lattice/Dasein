package com.latticeengines.pls.functionalframework;

import static com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated.usersInitialized;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PdSegmentEntityMgr;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;

public class PlsDeploymentTestNGBaseDeprecated extends PlsAbstractTestNGBaseDeprecated {

    protected static final Logger log = LoggerFactory.getLogger(PlsDeploymentTestNGBaseDeprecated.class);

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Value("${common.test.admin.url}")
    private String adminApi;

    @Autowired
    private InternalTestUserService internalTestUserService;

    @Autowired
    private PdSegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment(null, false);
    }

    protected void setupTestEnvironment(String productPrefix, Boolean forceInstallation)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        resetTenantsViaTenantConsole(productPrefix, forceInstallation);

        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
        modelSummaryParser = new ModelSummaryParser();
    }

    protected void resetTenantsViaTenantConsole(String productPrefix, Boolean forceInstallation) throws IOException {
        if (resetByAdminApi) {
            addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
            magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
            String url = "/pls/internal/testtenants/?forceinstall=" + String.valueOf(forceInstallation);
            if (productPrefix != null) {
                url += "&product=" + productPrefix;
            }
            String response = sendHttpPutForObject(magicRestTemplate, getRestAPIHostPort() + url, "", String.class);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(response);
            Assert.assertTrue(json.get("Success").asBoolean());
        }
    }

    protected void deleteUserWithUsername(String username) {
        internalTestUserService.deleteUserWithUsername(username);
    }

    protected void setupDbWithMarketoSMB(String tenantId, String tenantName) throws Exception {
        setupDbWithMarketoSMB(tenantId, tenantName, true, true);
    }

    protected void setupDbWithMarketoSMB(String tenantId, String tenantName, boolean createSummaries,
            boolean createSegments) throws Exception {
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantName);
        if (tenantService.hasTenantId(tenantId)) {
            tenantEntityMgr.delete(tenant);
        }
        tenantService.registerTenant(tenant);

        ModelSummary summary1 = null;
        if (createSummaries) {
            summary1 = getDetails(tenant, "marketo");
            String[] tokens = summary1.getLookupId().split("\\|");
            tokens[0] = tenantId;
            tokens[1] = "Q_PLS_Modeling_" + tenantId;
            summary1.setLookupId(String.format("%s|%s|%s", tokens[0], tokens[1], tokens[2]));

            String modelId = summary1.getId();
            summary1.setLastUpdateTime(System.currentTimeMillis());
            ModelSummary summary = modelSummaryProxy.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryProxy.deleteByModelId(tenantId, summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryProxy.create(tenantId, summary1);
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

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }

    private ModelSummary getDetails(Tenant tenant, String suffix) throws Exception {
        String file = String.format("com/latticeengines/pls/functionalframework/modelsummary-%s.json", suffix);
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(file);
        String contents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        String fakePath = String.format("/user/s-analytics/customers/%s-%s", tenant.getId(), suffix);
        ModelSummary summary = modelSummaryParser.parse(fakePath, contents);
        summary.setTenant(tenant);
        return summary;
    }

    protected void setupMarketoEloquaTestEnvironment() throws Exception {
        setTestingTenants();
        setupDbUsingDefaultTenantIds();
        setupUsers();
    }

    protected void setupDbUsingDefaultTenantIds() throws Exception {
        setupDbUsingDefaultTenantIds(true, true);
    }

    protected void setupDbUsingDefaultTenantIds(boolean useTenant1, boolean useTenant2) throws Exception {
        setupDbUsingDefaultTenantIds(useTenant1, useTenant2, true, true);
    }

    protected void setupDbUsingDefaultTenantIds(boolean useTenant1, boolean useTenant2, boolean createSummaries,
            boolean createSegments) throws Exception {
        String tenant1Id = useTenant1 ? testingTenants.get(0).getId() : null;
        String tenant1Name = useTenant1 ? testingTenants.get(0).getName() : null;
        String tenant2Id = useTenant2 ? testingTenants.get(1).getId() : null;
        String tenant2Name = useTenant2 ? testingTenants.get(1).getName() : null;
        setupDbWithMarketoSMB(tenant1Id, tenant1Name, createSummaries, createSegments);
        setupDbWithEloquaSMB(tenant2Id, tenant2Name, createSummaries, createSegments);
    }

    protected void setupDbWithEloquaSMB(String tenantId, String tenantName) throws Exception {
        setupDbWithEloquaSMB(tenantId, tenantName, true, true);
    }

    protected void setupDbWithEloquaSMB(String tenantId, String tenantName, boolean createSummaries,
            boolean createSegments) throws Exception {
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantName);
        if (tenantService.hasTenantId(tenantId)) {
            tenantEntityMgr.delete(tenant);
        }
        tenantService.registerTenant(tenant);

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

            summary2.setLastUpdateTime(System.currentTimeMillis());
            String modelId = summary2.getId();
            ModelSummary summary = modelSummaryProxy.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryProxy.deleteByModelId(tenantId, summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryProxy.create(tenantId, summary2);
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

    protected void setupUsers() throws Exception {
        if (usersInitialized) {
            return;
        }
        setTestingTenants();
        loginTestingUsersToMainTenant();
        usersInitialized = true;
    }
}
