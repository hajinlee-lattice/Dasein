package com.latticeengines.pls.controller;

import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class CrmCredentialResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private final String tenantId = "PLSCrmConfigDeployment";
    
    private final String contractId = "PLSCrmConfigDeployment";

    private final String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;

    private CustomerSpace customerSpace;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
        customerSpace = CustomerSpace.parse(tenantId);
        restTemplate.setErrorHandler(statusErrorHandler);
        switchToSuperAdmin();
    }

    @AfterClass(groups = { "deployment" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
        camille.delete(path);
    }

    @Test(groups = "deployment")
    public void verifyCredentialUsingEai() {
        // sfdc production
        FeatureFlagDefinition def = new FeatureFlagDefinition();
        def.setConfigurable(true);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
        FeatureFlagClient.setEnabled(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), true);

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" +  customerSpace.toString() +   "&isProduction=true&verifyOnly=true",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // beware that password might change for this sandbox user
        crmCredential = new CrmCredential();
        crmCredential.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" +  customerSpace.toString() +   "&isProduction=false&verifyOnly=true",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00DM0000001dg3uMAA");
        FeatureFlagClient.removeFromSpace(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyCredentialUsingEai")
    public void getCredentialUsingEai() {
        CrmCredential newCrmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" +  customerSpace.toString() +   "&&isProduction=true", CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getUserName(), "apeters-widgettech@lattice-engines.com");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
        Assert.assertEquals(newCrmCredential.getSecurityToken(), "oIogZVEFGbL3n0qiAp6F66TC");

        newCrmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" +  customerSpace.toString() +   "&&isProduction=false", CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00DM0000001dg3uMAA");
        Assert.assertEquals(newCrmCredential.getUserName(), "tsanghavi@lattice-engines.com.sandbox2");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
        Assert.assertEquals(newCrmCredential.getSecurityToken(), "5aGieJUACRPQ21CG3nUwn8iz");
    }

}
