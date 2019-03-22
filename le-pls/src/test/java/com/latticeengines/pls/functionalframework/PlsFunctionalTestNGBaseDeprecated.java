package com.latticeengines.pls.functionalframework;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;

public class PlsFunctionalTestNGBaseDeprecated extends PlsAbstractTestNGBaseDeprecated {

    protected static boolean usersInitialized = false;

    protected static final String BISAP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000WjNY";
    protected static final String BISLP_URL = "https://login.salesforce.com/packaging/installPackage.apexp?p0=04tF0000000Kk28";

    private static Map<AccessLevel, User> testingUsers;

    @Autowired
    private InternalTestUserService internalTestUserService;

    private ModelSummaryParser modelSummaryParser = new ModelSummaryParser();;

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

    protected boolean createTenantByRestCall(String tenantName) {
        Tenant tenant = new Tenant();
        tenant.setId(tenantName);
        tenant.setName(tenantName);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        return magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                new HashMap<>());
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

    protected User getTheTestingUserAtLevel(AccessLevel level) {
        if (testingUsers == null || testingUsers.isEmpty()) {
            testingUsers = internalTestUserService
                    .createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel(testingTenants);
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
            mainTestTenant = testingTenants.get(0);
            ALTERNATIVE_TESTING_TENANT = testingTenants.get(1);
        }
    }

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }
}
