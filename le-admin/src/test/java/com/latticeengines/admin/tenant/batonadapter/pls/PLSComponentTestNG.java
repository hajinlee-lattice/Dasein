package com.latticeengines.admin.tenant.batonadapter.pls;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
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

public class PLSComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException {
        String testAdminUsername = "pls-installer-tester@lattice-engines.com";
        String testAdminPassword = "admin";

        String PLSTenantId = String.format("%s.%s.%s",
                contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        try {
            deletePLSAdminUser(PLSTenantId, testAdminUsername);
            deletePLSTestTenant(PLSTenantId);
        } catch (Exception e) {
            // ignore
        }

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        int numOfRetries = 10;
        BootstrapState state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, "PLS");
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        if (!state.state.equals(BootstrapState.State.OK)) {
            System.out.println(state.errorMessage);
        }

        Assert.assertEquals(state.state, BootstrapState.State.OK);

        Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, PLSTenantId));

        deletePLSAdminUser(PLSTenantId, testAdminUsername);
        deletePLSTestTenant(PLSTenantId);
    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException {
        String testAdminUsername = "pls-installer-tester@lattice-engines.com";

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        int numOfRetries = 10;
        BootstrapState.State state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, "PLS").state;
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        Assert.assertEquals(state, BootstrapState.State.OK);

        SerializableDocumentDirectory sDir = tenantService.getTenantServiceConfig(contractId, tenantId, "PLS");

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
        createCommonTenant();
    }

    private void createTestTenant(String tenantId, String tenantName, CRMTopology topology)
            throws Exception{
        loginAD();

        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "PLS Test tenant";
        props.displayName = TestContractId + " " + tenantName;
        props.topology = topology.name();
        props.product = "LPA";
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
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        // send to bootstrapper message queue
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

    private void createCommonTenant() throws Exception {

        String contractId = "CommonTestContract";
        String tenantId = "TestTenant";
        loginAD();

        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "PLS Test tenant";
        props.displayName = "Lattice Internal Test Tenant";
        props.topology = "MARKETO";
        props.product = "LPA";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));

        SpaceConfiguration spaceConfig = tenantService.getDefaultSpaceConfig();

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
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        // send to bootstrapper message queue
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

    @Override
    protected String getServiceName() { return PLSComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "pls_expected.json"; }

    private void deletePLSAdminUser(String username, String tenantId) {
        try {
            magicRestTemplate.delete(getPlsHostPort()
                    + String.format(
                    "/pls/internal/users?tenants=[\"%s\"]&namepattern=%s",
                    URIUtil.encodeQuery(username), tenantId));
        } catch (URIException e) {
            Assert.fail("could not encode the username");
        }
    }

    private void deletePLSTestTenant(String tenantId) {
        magicRestTemplate.delete(getPlsHostPort()
                + String.format( "/pls/admin/tenants/%s", tenantId));
    }

    private UserDocument loginAndAttach(String username, String password, String tenantId) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getPlsHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

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

        return restTemplate.postForObject(getPlsHostPort() + "/pls/attach", tenant,
                UserDocument.class);
    }

}
