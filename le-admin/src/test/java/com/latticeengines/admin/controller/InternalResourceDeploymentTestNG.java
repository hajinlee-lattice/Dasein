package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.UserService;

public class InternalResourceDeploymentTestNG extends AdminDeploymentTestNGBase{

    @Inject
    private UserService userService;

    @Inject
    private ServiceService serviceService;

    @Inject
    private TenantService tenantService;

    private String email = "lpl@lattice-engines.com";

    private static final Logger log = LoggerFactory.getLogger(InternalResourceDeploymentTestNG.class);
    /**
     * In setup, orchestrateForInstall a full tenant.
     **/
    @BeforeClass(groups = "deployment")
    public void setup() {
        TestTenantId = TestContractId;

        loginAD();
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        provisionTestTenants();
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        cleanup();
    }

    private void provisionTestTenants() {

        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "A test tenant across all component provisioned by tenant console through deployment tests.";
        tenantProperties.displayName = "DevelopTest:Tenant for testing1";
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "{\"Dante\":true,\"EnableDataEncryption\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        //spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(CRMTopology.ELOQUA);

        SerializableDocumentDirectory PLSconfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        for (SerializableDocumentDirectory.Node node : PLSconfig.getNodes()) {
            if (node.getNode().contains("SuperAdminEmails")) {
                node.setData("[]");
            } else if (node.getNode().contains("LatticeAdminEmails")) {
                node.setData("[]");
            }
        }
        PLSconfig.setRootPath("/" + PLSComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(PLSconfig);

        // Orchestrate tenant
        TenantRegistration reg = new TenantRegistration();
        reg.setContractInfo(new ContractInfo(new ContractProperties()));
        reg.setTenantInfo(tenantInfo);
        reg.setSpaceInfo(spaceInfo);
        reg.setSpaceConfig(spaceConfiguration);
        reg.setConfigDirectories(configDirs);

        String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(), TestTenantId, TestContractId);
        Boolean created = restTemplate.postForObject(url, reg, Boolean.class);
        Assert.assertNotNull(created);
        Assert.assertTrue(created);

    }

    @Test(groups = { "deployment" }, enabled = false)
    public void testUpdateUserStatusBaseOnEmails(){
        BootstrapState state = waitUntilStateIsNotInitial(TestContractId, TestTenantId, PLSComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }

        String payload = "one@lattice-engines.com, two@test.lattice-engines.com, lpl@lattice-engines.com ";

        String userName = "lpl@lattice-engines.com";
        User user = new User();
        user.setActive(true);
        user.setEmail(email);
        user.setFirstName("Robin");
        user.setLastName("Liu");
        user.setUsername(userName);

        Credentials creds = new Credentials();
        creds.setUsername(userName);
        creds.setPassword("lattice");
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        String tenant = String.format("%s.%s.%s", TestTenantId, TestContractId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        userRegistrationWithTenant.setTenant(tenant);
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        userRegistration.setUser(user);
        userRegistration.setCredentials(creds);
        userService.addAdminUser(userRegistrationWithTenant);

        GlobalAuthUser userAfterAdd = userService.findByEmailNoJoin(email);
        Assert.assertNotNull(userAfterAdd);
        Assert.assertTrue(userService.inTenant(tenant, userName));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(payload, headers);
        String url = getRestHostPort() + "/admin/internal/services/deactiveUserStatus";
        restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Boolean.class);

        GlobalAuthUser userAfterDeactive = userService.findByEmailNoJoin(email);
        Assert.assertNotNull(userAfterDeactive);
        Assert.assertFalse(userAfterDeactive.getIsActive());
        Assert.assertFalse(userService.inTenant(tenant, userName));
    }

    /**
     * ==================================================
     * BEGIN: Tenant clean up methods
     * ==================================================
     */
    public void cleanup() throws Exception {
        try {
            userService.deleteUserByEmail(email);
            deleteTenant(TestTenantId, TestTenantId);
        } catch (Exception e) {
            log.error("clean up tenant error!");
        }
    }
}
