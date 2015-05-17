package com.latticeengines.admin.tenant.batonadapter.pls;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;

@Component
public class PLSComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    private final static String testAdminUsername = "pls-installer-tester@lattice-engines.com";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    public RestTemplate plsRestTemplate = new RestTemplate();

    @AfterClass(groups = {"deployment", "functional"}, alwaysRun = true)
    public void tearDown() throws Exception {
        super.tearDown();
        String PLSTenantId = String.format("%s.%s.%s",
                contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        deletePLSAdminUser(testAdminUsername);
        deletePLSTestTenant(PLSTenantId);
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException {
        String testAdminPassword = "admin";

        String PLSTenantId = String.format("%s.%s.%s",
                contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/SuperAdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/LatticeAdminEmails"));
        node.getDocument().setData("[ ]");

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, PLSTenantId));
    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException {
        String testAdminUsername = "pls-installer-tester@lattice-engines.com";

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/SuperAdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/LatticeAdminEmails"));
        node.getDocument().setData("[ ]");

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        SerializableDocumentDirectory sDir = tenantService.getTenantServiceConfig(contractId, tenantId,
                PLSComponent.componentName);

        for (SerializableDocumentDirectory.Node sNode : sDir.getNodes()) {
            if (sNode.getNode().equals("AdminEmails")) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    JsonNode jNode = mapper.readTree(sNode.getData());
                    Assert.assertTrue(jNode.isArray());
                    Assert.assertEquals(jNode.get(0).asText(), testAdminUsername);
                } catch (IOException e) {
                    throw new AssertionError("Could not parse the data stored in ZK.");
                }
            }
        }

    }

    @Test(groups = {"deployment", "functional"})
    public void installTestTenants() throws Exception {
        createTestTenant("Tenant1", "Tenant 1", CRMTopology.MARKETO);
        createTestTenant("Tenant2", "Tenant 2", CRMTopology.ELOQUA);
    }

    private void createTestTenant(String tenantId, String tenantName, CRMTopology topology)
            throws Exception{
        loginAD();

        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "PLS Test tenant";
        props.displayName = TestContractId + " " + tenantName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));
        SpaceConfiguration spaceConfig = new SpaceConfiguration();
        spaceConfig.setTopology(topology);

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);
        reg.setSpaceConfig(spaceConfig);

        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId, false, reg);

        String testAdminUsername = "bnguyen@lattice-engines.com";

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/SuperAdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/LatticeAdminEmails"));
        node.getDocument().setData("[ ]");

        // send to bootstrapper message queue
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

    @Override
    protected String getServiceName() { return PLSComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "pls_expected.json"; }

    private void deletePLSAdminUser(String username) {
        if (globalUserManagementService.getUserByUsername(username) != null) {
            globalUserManagementService.deleteUser(username);
        }
    }
    
    
    @SuppressWarnings("unchecked")
	public void deletePLSTestTenant(String tenantId) {
        try {
            List<Tenant> tenants = magicRestTemplate.getForObject(getPlsHostPort() + "/pls/admin/tenants", List.class);
            for (Tenant tenant: tenants) {
                if (((Tenant) tenant).getId().equals(tenantId)) return;
            }
            magicRestTemplate.delete(getPlsHostPort()
                    + String.format("/pls/admin/tenants/%s", tenantId));
        } catch (Exception e) {
            // ignore
        }
    }

    public UserDocument loginAndAttach(String username, String password, String tenantId) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = plsRestTemplate.postForObject(getPlsHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        plsRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        List<Tenant> tenants = doc.getResult().getTenants();

        if (tenants == null || tenants.isEmpty()) { Assert.fail("No tenant for the login user " + username); }

        Tenant tenant = null;
        for (Tenant tenant1: doc.getResult().getTenants()) {
            if (tenant1.getId().equals(tenantId)) {
                tenant = tenant1;
                break;
            }
        }

        Assert.assertNotNull(tenant);

        return plsRestTemplate.postForObject(getPlsHostPort() + "/pls/attach", tenant, UserDocument.class);
    }

}
