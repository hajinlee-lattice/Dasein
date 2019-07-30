package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.entitymanager.PdSegmentEntityMgr;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlsDeploymentTestNGBase.class);

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Autowired
    private PdSegmentEntityMgr segmentEntityMgr;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    protected Tenant marketoTenant;
    protected Tenant eloquaTenant;

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithGATenants(1);
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironmentWithGATenants(int numTenants) throws Exception {
        testBed.bootstrap(numTenants);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void setupTestEnvironmentWithExistingTenant(String tenantId)
            throws NoSuchAlgorithmException, KeyManagementException {
        turnOffSslChecking();
        testBed.bootstrap(0);
        testBed.useExistingTenantAsMain(tenantId);
        initializeTestVariables();
    }

    protected void setupTestEnvironmentWithOneTenant()
            throws NoSuchAlgorithmException, KeyManagementException {
        turnOffSslChecking();
        testBed.bootstrap(1);
        initializeTestVariables();
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)
            throws NoSuchAlgorithmException, KeyManagementException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product);
        initializeTestVariables();
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product,
            Map<String, Boolean> featureFlagMap) throws NoSuchAlgorithmException, KeyManagementException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product, featureFlagMap);
        initializeTestVariables();
    }

    private void initializeTestVariables() {
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void deleteUserByRestCall(String username) {
        switchToSuperAdmin();
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        restTemplate.delete(url);
    }

    protected void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        ((GlobalAuthDeploymentTestBed) testBed).attachProtectedProxy(proxy);
    }

    protected JobStatus waitForWorkflowStatus(WorkflowProxy workflowProxy, String applicationId, boolean running) {
        return waitForWorkflowStatus(workflowProxy, applicationId, running, mainTestTenant);
    }

    protected JobStatus waitForWorkflowStatus(WorkflowProxy workflowProxy, String applicationId, boolean running,
                                              Tenant tenant) {
        int retryOnException = 4;
        Job job = null;

        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                        CustomerSpace.parse(tenant.getId()).toString());
            } catch (Exception e) {
                System.out.println(String.format("Workflow job exception: %s", e.getMessage()));

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    protected void setupMarketoEloquaTestEnvironment() throws Exception {
        testBed.bootstrap(2);
        setupDbUsingDefaultTenantIds();
        switchToSuperAdmin();
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

    protected void setupDbWithMarketoSMB(Tenant tenant, boolean createSummaries, boolean createSegments)
            throws Exception {

        ModelSummary summary1 = null;
        if (createSummaries) {
            summary1 = getDetails(tenant, "marketo");
            String[] tokens = summary1.getLookupId().split("\\|");
            tokens[1] = "Q_PLS_Modeling_" + tenant.getId();
            summary1.setLookupId(String.format("%s|%s|%s", tokens[0], tokens[1], tokens[2]));

            String modelId = summary1.getId();
            ModelSummary summary = modelSummaryProxy.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryProxy.deleteByModelId(tenant.getId(), summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryProxy.create(tenant.getId(), summary1);
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
            summary2.setApplicationId(modelAppId);
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
            ModelSummary summary = modelSummaryProxy.retrieveByModelIdForInternalOperations(modelId);
            if (summary != null) {
                setupSecurityContext(summary);
                modelSummaryProxy.deleteByModelId(tenant.getId(), summary.getId());
            }
            setupSecurityContext(tenant);
            modelSummaryProxy.create(tenant.getId(), summary2);
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

    protected void setupSecurityContext(ModelSummary summary) { setupSecurityContext(summary.getTenant()); }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }

    protected LedpResponseErrorHandler getErrorHandler() {
        return testBed.getErrorHandler();
    }
}
