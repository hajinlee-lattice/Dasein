package com.latticeengines.pls.functionalframework;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class PlsFunctionalTestNGBase extends PlsAbstractTestNGBase {

    protected static boolean usersInitialized = false;

    protected static final String passwordTester = "pls-password-tester@test.lattice-engines.ext";
    protected static final String passwordTesterPwd = "Lattice123";

    protected static final String BISAP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000WjNY";
    protected static final String BISLP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000Kk28";

    private static Map<AccessLevel, User> testingUsers;

    @Autowired
    private InternalTestUserService internalTestUserService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private QuotaEntityMgr quotaEntityMgr;
    
    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;
    
    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @Value("${pls.test.functional.api:http://localhost:8080/}")
    private String hostPort;

    protected void createUser(String username, String email, String firstName, String lastName) {
        internalTestUserService.createUser(username, email, firstName, lastName);
    }

    protected void createUser(String username, String email, String firstName, String lastName, String password) {
        internalTestUserService.createUser(username, email, firstName, lastName, password);
    }

    protected void deleteUserWithUsername(String username) {
        internalTestUserService.deleteUserWithUsername(username);
    }

    protected boolean createTenantByRestCall(String tenantName) {
        Tenant tenant = new Tenant();
        tenant.setId(tenantName);
        tenant.setName(tenantName);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        return magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                new HashMap<>());
    }

    protected boolean createAdminUserByRestCall(String tenant, String username, String email, String firstName,
            String lastName, String password) {
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant(tenant);
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        User user = new User();
        user.setActive(true);
        user.setEmail(email);
        user.setFirstName(firstName);
        user.setLastName(lastName);
        user.setUsername(username);
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        userRegistration.setUser(user);
        userRegistration.setCredentials(creds);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        return magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users", userRegistrationWithTenant,
                Boolean.class);
    }

    @Override
    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
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

    protected void setUpMarketoEloquaTestEnvironment() throws Exception {
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

    protected void setupUsers() throws Exception {
        if (usersInitialized) {
            return;
        }

        setTestingTenants();
        
        createUser(adminUsername, adminUsername, "Super", "User", adminPasswordHash);
        createUser(generalUsername, generalUsername, "General", "User");

        for (Tenant tenant : testingTenants) {
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), adminUsername);
            userService.assignAccessLevel(AccessLevel.INTERNAL_USER, tenant.getId(), generalUsername);
            userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, tenant.getId(), passwordTester);

            for (AccessLevel level : AccessLevel.values()) {
                User user = getTheTestingUserAtLevel(level);
                if (user != null) {
                    userService.assignAccessLevel(level, tenant.getId(), user.getUsername());
                }
            }
        }

        loginTestingUsersToMainTenant();

        usersInitialized = true;
    }

    protected User getTheTestingUserAtLevel(AccessLevel level) {
        if (testingUsers == null || testingUsers.isEmpty()) {
            testingUsers = internalTestUserService
                    .createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel();
        }
        return testingUsers.get(level);
    }

    @Override
    protected void setTestingTenants() {
        if (testingTenants == null || testingTenants.isEmpty()) {
            List<String> subTenantIds = Arrays.asList(contractId + "PLSTenant1", contractId + "PLSTenant2");
            testingTenants = new ArrayList<>();
            for (String subTenantId : subTenantIds) {
                String tenantId = CustomerSpace.parse(subTenantId).toString();
                if (!tenantService.hasTenantId(tenantId)) {
                    Tenant tenant = new Tenant();
                    tenant.setId(tenantId);
                    String name = subTenantId.endsWith("Tenant1") ? "Tenant 1" : "Tenant 2";
                    tenant.setName(contractId + " " + name);
                    tenantService.registerTenant(tenant);
                }
                Tenant tenant = tenantService.findByTenantId(tenantId);
                Assert.assertNotNull(tenant);
                testingTenants.add(tenant);
            }
            mainTestingTenant = testingTenants.get(0);
            ALTERNATIVE_TESTING_TENANT = testingTenants.get(1);
        }
    }

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }

    protected void setupSecurityContext(Tenant t) {
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        TicketAuthenticationToken token = Mockito.mock(TicketAuthenticationToken.class);
        Session session = Mockito.mock(Session.class);
        Tenant tenant = Mockito.mock(Tenant.class);
        Mockito.when(session.getTenant()).thenReturn(tenant);
        Mockito.when(tenant.getId()).thenReturn(t.getId());
        Mockito.when(tenant.getPid()).thenReturn(t.getPid());
        Mockito.when(token.getSession()).thenReturn(session);
        Mockito.when(securityContext.getAuthentication()).thenReturn(token);
        SecurityContextHolder.setContext(securityContext);
    }

    protected void cleanupTargetMarketDB() {
        List<TargetMarket> targetMarkets = this.targetMarketEntityMgr.getAllTargetMarkets();
        for (TargetMarket targetMarket : targetMarkets) {
            if (targetMarket.getName().startsWith("TEST")) {
                this.targetMarketEntityMgr.deleteTargetMarketByName(targetMarket.getName());
            }
            this.targetMarketEntityMgr.deleteTargetMarketByName(targetMarket.getName());
        }
    }
        
    protected void cleanupQuotaDB() {
        List<Quota> quotas = this.quotaEntityMgr.getAllQuotas();
        for (Quota quota : quotas) {
            if (quota.getId().startsWith("TEST")) {
                this.quotaEntityMgr.deleteQuotaByQuotaId(quota.getId());
            }
        }
    }

}
