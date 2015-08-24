package com.latticeengines.admin.functionalframework;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.security.exposed.Constants;

public class AdminDeploymentTestNGBase extends AdminAbstractTestNGBase {

    @Value("${admin.test.deployment.api}")
    protected String remoteHostPort;

    @Override
    protected String getRestHostPort() {
        return remoteHostPort.endsWith("/") ? remoteHostPort.substring(0, remoteHostPort.length() - 1) : remoteHostPort;
    }

    @BeforeClass(groups = {"deployment"} )
    public void setup() throws Exception {
        loginAD();

        TestTenantId = TestContractId;

        String podId = CamilleEnvironment.getPodId();
        Assert.assertNotNull(podId);

        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(TestContractId, TestTenantId);

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
    }

    @AfterClass(groups = {"deployment"})
    public void tearDown() throws Exception {
        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            //ignore
        }
    }

}
