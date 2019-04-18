package com.latticeengines.admin.tenant.batonadapter.pls;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HttpClientUtils;
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
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;

@Component
public class PLSComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PLSComponentDeploymentTestNG.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    public RestTemplate plsRestTemplate = HttpClientUtils.newRestTemplate();

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

        node = confDir.get(new Path("/DataCloudLicense/HG"));
        node.getDocument().setData("10");
        return confDir;
    }

    @Test(groups = "deployment")
    public void testInstallation() {
        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        // try to set status and tenantType in tenant properties znode
        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(new TenantProperties());
        CustomerSpaceInfo customerSpaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
        tenantInfo.properties.status = TenantStatus.ACTIVE.name();
        tenantInfo.properties.tenantType = TenantType.CUSTOMER.name();
        tenantInfo.properties.displayName = tenantId;
        tenantEntityMgr.createTenant(contractId, tenantId, contractInfo, tenantInfo, customerSpaceInfo);

        // send to bootstrapper message queue
        bootstrap(getPLSDocumentDirectory());
        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        changeStatus(contractId, tenantId);
        Assert.assertNotNull(loginAndAttach(testAdminUsername, testAdminPassword, PLSTenantId));

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, PLSComponent.componentName);

        try {
            Camille camille = CamilleEnvironment.getCamille();
            DocumentDirectory doc = camille.getDirectory(servicePath);
            DocumentDirectory.Node node = doc.get("/DataCloudLicense/HG");
            Assert.assertNotNull(node);
            Assert.assertEquals("10", node.getDocument().getData());
            camille.delete(servicePath);
        } catch (Exception e) {
            // ignore
        }
        bootstrap(getPLSDocumentDirectory());
        state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
            changeStatus(contractId, tenantId);
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
            log.error(e.getMessage(), e);
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
