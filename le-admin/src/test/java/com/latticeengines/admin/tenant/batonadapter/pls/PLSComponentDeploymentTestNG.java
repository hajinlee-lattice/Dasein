package com.latticeengines.admin.tenant.batonadapter.pls;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;

@Component
public class PLSComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    private final static String testAdminUsername = "pls-installer-tester@lattice-engines.com";
    private final static String testAdminPassword = Base64Utils.encodeBase64WithDefaultTrim(testAdminUsername);

    private final static Log log = LogFactory.getLog(PLSComponentDeploymentTestNG.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    public RestTemplate plsRestTemplate = new RestTemplate();

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        log.info("Start tearing down public class PLSComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase");
        super.tearDown();
        tearDown(contractId, tenantId);
    }

    public void tearDown(String contractId, String tenantId) throws Exception {
        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        deletePLSAdminUser(testAdminUsername);
        deletePLSTestTenant(PLSTenantId);
    }

    public DocumentDirectory getPLSDocumentDirectory() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/SuperAdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/LatticeAdminEmails"));
        node.getDocument().setData("[ ]");

        node = confDir.get(new Path("/ExternalAdminEmails"));
        node.getDocument().setData("[ ]");

        node = confDir.get(new Path("/ThirdPartyUserEmails"));
        node.getDocument().setData("[ ]");
        return confDir;
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException {

        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        // send to bootstrapper message queue
        bootstrap(getPLSDocumentDirectory());
        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, PLSTenantId));

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, PLSComponent.componentName);
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }
        bootstrap(getPLSDocumentDirectory());
        state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
            Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, PLSTenantId));
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }

    }

    @Override
    protected String getServiceName() {
        return PLSComponent.componentName;
    }

    private void deletePLSAdminUser(String username) {
        if (globalUserManagementService.getUserByUsername(username) != null) {
            globalUserManagementService.deleteUser(username);
        }
    }

    public void deletePLSTestTenant(String tenantId) {
        try {
            addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
            magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
            magicRestTemplate.delete(getPlsHostPort() + String.format("/pls/admin/tenants/%s", tenantId));
        } catch (Exception e) {
            log.error(e);
        }
    }

    public UserDocument loginAndAttach(String username, String password, String tenantId) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = plsRestTemplate.postForObject(getPlsHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        plsRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

        List<Tenant> tenants = doc.getResult().getTenants();

        if (tenants == null || tenants.isEmpty()) {
            Assert.fail("No tenant for the login user " + username);
        }

        Tenant tenant = null;
        for (Tenant tenant1 : doc.getResult().getTenants()) {
            if (tenant1.getId().equals(tenantId)) {
                tenant = tenant1;
                break;
            }
        }

        Assert.assertNotNull(tenant);

        return plsRestTemplate.postForObject(getPlsHostPort() + "/pls/attach", tenant, UserDocument.class);
    }

}
