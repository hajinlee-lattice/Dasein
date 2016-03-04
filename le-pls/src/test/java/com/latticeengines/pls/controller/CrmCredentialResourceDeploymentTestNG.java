package com.latticeengines.pls.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;

public class CrmCredentialResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private CustomerSpace customerSpace;
    private static String tenant;
    private static Tenant tenantToAttach;

    private static final Log log = LogFactory.getLog(CrmCredentialResourceDeploymentTestNG.class);

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        System.out.println("Deleting existing test tenants ...");
        deleteTwoTenants();

        System.out.println("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironment("pd", true);

        System.out.println("Setting up testing users ...");
        tenantToAttach = testingTenants.get(1);
        if (tenantToAttach.getName().contains("Tenant 1")) {
            tenantToAttach = testingTenants.get(0);
        }
        tenant = tenantToAttach.getId();
        customerSpace = CustomerSpace.parse(tenant);
        UserDocument doc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(doc);

        log.info("Test environment setup finished.");
        System.out.println("Test environment setup finished.");
    }

    private void deleteTwoTenants() throws Exception {
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
        }
    }

    @AfterClass(groups = { "deployment" })
    public void afterClass() throws Exception {
//        Camille camille = CamilleEnvironment.getCamille();
//        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
//        camille.delete(path);
    }

    @Test(groups = "deployment")
    public void verifyCredentialUsingEai() {
        // sfdc production
//        FeatureFlagDefinition def = FeatureFlagClient.getDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL
//                .getName());
//        def.setConfigurable(true);
//        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
//        FeatureFlagClient.setEnabled(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), true);

        switchToThirdPartyUser();
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" + customerSpace.toString() + "&isProduction=true&verifyOnly=true",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // TODO: resume once get sandbox credentials
        // // beware that password might change for this sandbox user
        // crmCredential = new CrmCredential();
        // crmCredential.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        // crmCredential.setPassword("Happy2010");
        // crmCredential.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        // newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
        // + "/pls/credentials/sfdc?tenantId=" + customerSpace.toString() +
        // "&isProduction=false&verifyOnly=true",
        // crmCredential, CrmCredential.class);
        // Assert.assertEquals(newCrmCredential.getOrgId(),
        // "00DM0000001dg3uMAA");
//        FeatureFlagClient.removeFromSpace(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
//        def.setConfigurable(false);
//        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyCredentialUsingEai")
    public void getCredentialUsingEai() {
        CrmCredential newCrmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" + customerSpace.toString() + "&&isProduction=true",
                CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getUserName(), "apeters-widgettech@lattice-engines.com");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
        Assert.assertEquals(newCrmCredential.getSecurityToken(), "oIogZVEFGbL3n0qiAp6F66TC");

        // TODO: resume once get sandbox credentials
        // newCrmCredential = restTemplate.getForObject(getRestAPIHostPort()
        // + "/pls/credentials/sfdc?tenantId=" + customerSpace.toString() +
        // "&&isProduction=false", CrmCredential.class);
        // Assert.assertEquals(newCrmCredential.getOrgId(),
        // "00DM0000001dg3uMAA");
        // Assert.assertEquals(newCrmCredential.getUserName(),
        // "tsanghavi@lattice-engines.com.sandbox2");
        // Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
        // Assert.assertEquals(newCrmCredential.getSecurityToken(),
        // "5aGieJUACRPQ21CG3nUwn8iz");
    }

}
