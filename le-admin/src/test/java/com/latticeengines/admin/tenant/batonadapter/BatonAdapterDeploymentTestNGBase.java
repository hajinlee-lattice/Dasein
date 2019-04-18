package com.latticeengines.admin.tenant.batonadapter;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;

/**
 * besides the same setup and teardown as AdminFunctionalTestNGBase, we also
 * register the testing component's installer, in case it has not been
 * registered already by ServiceServiceImpl
 */
public abstract class BatonAdapterDeploymentTestNGBase extends AdminDeploymentTestNGBase {

    protected static final String testAdminUsername = "pls-installer-tester@lattice-engines.com";
    protected static final String testAdminPassword = Base64Utils.encodeBase64WithDefaultTrim(testAdminUsername);

    protected String contractId, tenantId, serviceName;
    private static final long TIMEOUT = 180000L;
    private static final long WAIT_INTERVAL = 3000L;

    @Value("${common.test.pls.url}")
    private String plsHostPort;

    protected RestTemplate plsRestTemplate = HttpClientUtils.newRestTemplate();

    @BeforeClass(groups = { "deployment", "functional", "lp2" })
    public void setup() throws Exception {
        serviceName = getServiceName();
        contractId = NamingUtils.timestamp(this.getClass().getSimpleName());
        tenantId = contractId;

        loginAD();
        createTenant(contractId, tenantId);

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
    }

    @AfterClass(groups = { "deployment", "functional", "lp2" }, alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test(groups = { "deployment", "functional", "lp2" })
    public void testGetDefaultConfig() throws Exception {
        verifyDefaultConfig();
    }

    protected abstract String getServiceName();

    protected void bootstrap(DocumentDirectory confDir) {
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

    private void verifyDefaultConfig() {
        loginAD();
        String url = String.format("%s/admin/services/%s/default", getRestHostPort(), serviceName);
        SerializableDocumentDirectory serializableDir = restTemplate.getForObject(url,
                SerializableDocumentDirectory.class);
        Assert.assertNotNull(serializableDir);

        DocumentDirectory dir = SerializableDocumentDirectory.deserialize(serializableDir);
        dir.makePathsLocal();
        serializableDir = new SerializableDocumentDirectory(dir);
        DocumentDirectory metaDir = batonService.getConfigurationSchema(serviceName);
        serializableDir.applyMetadata(metaDir);

        Assert.assertNotNull(serializableDir);
    }

    protected String getPlsHostPort() {
        return plsHostPort;
    }

    public BootstrapState waitForSuccess(String componentName) throws InterruptedException {
        long numOfRetries = TIMEOUT / WAIT_INTERVAL;
        BootstrapState state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, componentName);
            numOfRetries--;
            Thread.sleep(WAIT_INTERVAL);
        } while (state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        if (!state.state.equals(BootstrapState.State.OK)) {
            System.out.println(state.errorMessage);
        }
        return state;
    }

    public UserDocument loginAndAttachPls(String username, String password, String tenantId) {
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
