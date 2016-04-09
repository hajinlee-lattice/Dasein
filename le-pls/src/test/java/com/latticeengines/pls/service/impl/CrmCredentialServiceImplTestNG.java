package com.latticeengines.pls.service.impl;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.functionalframework.SourceCredentialValidationServlet;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.proxy.exposed.eai.ValidateCredentialProxy;
import com.latticeengines.testframework.rest.StandaloneHttpServer;

@SuppressWarnings("unused")
public class CrmCredentialServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private BatonService batonService;

    @Autowired
    private CrmCredentialService crmService;

    private final String contractId = "PLSCrmConfig";
    private final String tenantId = "PLSCrmConfig";
    private final String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;
    private final String fullId = String.format("%s.%s.%s", contractId, tenantId, spaceId);

    private static final String SFDC_PROD_USER = "apeters-widgettech@lattice-engines.com";
    private static final String SFDC_PROD_ORG_ID = "00D80000000KvZoEAK";
    private static final String SFDC_PROD_PASSWD = "Happy2010";
    private static final String SFDC_PROD_TOKEN = "oIogZVEFGbL3n0qiAp6F66TC";

    private static final String SFDC_SANDBOX_USER = "tsanghavi@lattice-engines.com.sandbox2";
    private static final String SFDC_SANDBOX_ORG_ID = "00DM0000001dg3uMAA";
    private static final String SFDC_SANDBOX_PASSWD = "Happy2010";
    private static final String SFDC_SANDBOX_TOKEN = "5aGieJUACRPQ21CG3nUwn8iz";

    private static final String MKTO_USER = "latticeenginessandbox1_9026948050BD016F376AE6";
    private static final String MKTO_URL = "https://na-sj02.marketo.com/soap/mktows/2_0";
    private static final String MKTO_PASSSWD = "41802295835604145500BBDD0011770133777863CA58";

    private static final String ELQ_USER = "Matt.Sable";
    private static final String ELQ_PASSWORD = "Lattice2";
    private static final String ELQ_COMPANY = "TechnologyPartnerLatticeEngines";

    private StandaloneHttpServer httpServer;

    private CustomerSpace customerSpace;

    @Autowired
    private ValidateCredentialProxy validateCredentialProxy;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }
        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(contractId, tenantId, spaceId, spaceInfo);

        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        spaceConfiguration.setProducts(Collections.singletonList(LatticeProduct.LPA));
        batonService.setupSpaceConfiguration(contractId, tenantId, spaceId, spaceConfiguration);

        customerSpace = CustomerSpace.parse(tenantId);
        validateCredentialProxy.setMicroserviceHostPort("http://localhost:8082");
        httpServer = new StandaloneHttpServer();
        httpServer.init();
        httpServer.addServlet(new SourceCredentialValidationServlet(), "/eai/validatecredential/customerspaces/"
                + customerSpace.toString() + "/*");
        httpServer.start();
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
        camille.delete(path);
        httpServer.stop();
    }

    @Test(groups = "functional")
    public void verifyCredentialUsingEai() {
        // sfdc production
        FeatureFlagDefinition def = FeatureFlagClient.getDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
        def.setConfigurable(true);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
        FeatureFlagClient.setEnabled(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), true);

        CrmCredential crmCredential = sfdcProductionCredentials();
        CrmCredential newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE,
                crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_PROD_ORG_ID);

        //TODO: need to update sandbox credentials
//        // beware that password might change for this sandbox user
//        crmCredential = sfdcSandboxCredentials();
//        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE, crmCredential);
//        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_SANDBOX_ORG_ID);
        FeatureFlagClient.removeFromSpace(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
        def.setConfigurable(false);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
    }

    @Test(groups = "functional", dependsOnMethods = "verifyCredentialUsingEai")
    public void getCredentialUsingEai() {
        CrmCredential newCrmCredential = crmService.getCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_PROD_ORG_ID);
        Assert.assertEquals(newCrmCredential.getPassword(), SFDC_PROD_PASSWD);
        Assert.assertEquals(newCrmCredential.getSecurityToken(), SFDC_PROD_TOKEN);
        crmService.removeCredentials(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE);

//        newCrmCredential = crmService.getCredential(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE);
//        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_SANDBOX_ORG_ID);
//        Assert.assertEquals(newCrmCredential.getPassword(), SFDC_SANDBOX_PASSWD);
//        Assert.assertEquals(newCrmCredential.getSecurityToken(), SFDC_SANDBOX_TOKEN);
//        crmService.removeCredentials(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE);
    }

    @Test(groups = "functional")
    public void verifyCredentialWrongPassword() {
        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(SFDC_PROD_USER);
        crmCredential.setPassword("nope");
        crmCredential.setSecurityToken(SFDC_PROD_TOKEN);
        boolean encounteredException = false;
        try {
            crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE, crmCredential);
        } catch (Exception e) {
            encounteredException = true;
        }
        Assert.assertTrue(encounteredException, "Wrong password should cause exception while validating sfdc.");

        crmCredential = new CrmCredential();
        crmCredential.setUserName(SFDC_SANDBOX_USER);
        crmCredential.setPassword("nope");
        crmCredential.setSecurityToken(SFDC_SANDBOX_TOKEN);
        encounteredException = false;
        try {
            crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE, crmCredential);
        } catch (Exception e) {
            encounteredException = true;
        }
        Assert.assertTrue(encounteredException, "Wrong password should cause exception while validating sfdcsandbox.");
    }

    private CrmCredential sfdcProductionCredentials() {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(SFDC_PROD_USER);
        crmCredential.setPassword(SFDC_PROD_PASSWD);
        crmCredential.setSecurityToken(SFDC_PROD_TOKEN);
        return crmCredential;
    }

    private CrmCredential sfdcSandboxCredentials() {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(SFDC_SANDBOX_USER);
        crmCredential.setPassword(SFDC_SANDBOX_PASSWD);
        crmCredential.setSecurityToken(SFDC_SANDBOX_TOKEN);
        return crmCredential;
    }

}
