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
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
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

        deletePLSAdminUser(tenantId, testAdminUsername);
        deletePLSTestTenant(tenantId);

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
        } while (!state.equals(BootstrapState.State.OK) && numOfRetries > 0);

        Assert.assertEquals(state, BootstrapState.State.OK);

        Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, tenantId));

        deletePLSAdminUser(tenantId, testAdminUsername);
        deletePLSTestTenant(tenantId);
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
        } while (!state.equals(BootstrapState.State.OK) && numOfRetries > 0);

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

        return restTemplate.postForObject(getPlsHostPort() + "/pls/attach", doc.getResult().getTenants().get(0),
                UserDocument.class);
    }

}
