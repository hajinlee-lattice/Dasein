package com.latticeengines.admin.tenant.batonadapter;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.security.exposed.Constants;

import junit.framework.Assert;

/**
 * besides the same setup and teardown as AdminFunctionalTestNGBase,
 * we also register the testing component's installer,
 * in case it has not been registered already by ServiceServiceImpl
 */
public abstract class BatonAdapterDeploymentTestNGBase extends AdminFunctionalTestNGBase {

    protected String contractId, tenantId, serviceName;

    @Value("${pls.api.hostport}")
    private String plsHostPort;

    @BeforeClass(groups = {"deployment", "functional"})
    public void setup() throws Exception {
        serviceName = getServiceName();
        contractId = TestContractId + serviceName + "Contract";
        tenantId = TestContractId + serviceName + "Tenant";

        loginAD();
        cleanupZK();
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId);

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
    }

    @AfterClass(groups = {"deployment", "functional"}, alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
    }

    @Test(groups = {"deployment", "functional"})
    public void testGetDefaultConfig() throws Exception {
        verifyDefaultConfig();
    }

    protected abstract String getServiceName();

    protected void bootstrap(DocumentDirectory confDir) { super.bootstrap(contractId, tenantId, serviceName, confDir); }

    private void verifyDefaultConfig() {
        loginAD();
        String url = String.format("%s/admin/services/%s/default", getRestHostPort(), serviceName);
        SerializableDocumentDirectory serializableDir =
                restTemplate.getForObject(url, SerializableDocumentDirectory.class);
        Assert.assertNotNull(serializableDir);

        DocumentDirectory dir = SerializableDocumentDirectory.deserialize(serializableDir);
        dir.makePathsLocal();
        serializableDir = new SerializableDocumentDirectory(dir);
        DocumentDirectory metaDir = batonService.getConfigurationSchema(serviceName);
        serializableDir.applyMetadata(metaDir);

        Assert.assertNotNull(serializableDir);
    }

    protected String getPlsHostPort() { return plsHostPort; }

    public BootstrapState waitForSuccess(String componentName) throws InterruptedException{
        int numOfRetries = 10;
        BootstrapState state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, componentName);
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        if (!state.state.equals(BootstrapState.State.OK)) {
            System.out.println(state.errorMessage);
        }
        return state;
    }
}
