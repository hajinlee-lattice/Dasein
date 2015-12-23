package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.source.SourceCredentialType;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class EaiCredentialValidationServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private EaiCredentialValidationService eaiCredentialValidationService;

    @Value("${eai.test.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.test.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.test.salesforce.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    @Value("${eai.salesforce.sandbox.loginurl}")
    private String sandboxLoginUrl;

    private String customer = "SFDC-Eai-Customer";

    private String customerSpace = CustomerSpace.parse(customer).toString();

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        crmCredentialZKService.removeCredentials(customer, customer, true);
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customer, customer, "defaultspaceId", spaceInfo);
    }

    @Test(groups = "functional")
    public void testVerifyCrmCredentialInProduction() {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        crmCredential.setUrl(productionLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
        eaiCredentialValidationService.validateSourceCredential(customerSpace, CrmConstants.CRM_SFDC,
                SourceCredentialType.PRODUCTION);
    }

    @Test(groups = "functional", enabled = false)
    public void testVerifyCrmCredentialInSandbox() {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        crmCredential.setUrl(sandboxLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, false, crmCredential, true);
        eaiCredentialValidationService.validateSourceCredential(customerSpace, CrmConstants.CRM_SFDC,
                SourceCredentialType.SANDBOX);
    }

    @Test(groups = "functional")
    public void testVerifyBadCrmCredential() {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("some bad username");
        crmCredential.setPassword("some bad password");
        crmCredential.setSecurityToken("badtoken");
        crmCredential.setUrl(productionLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
        try {
            eaiCredentialValidationService.validateSourceCredential(customerSpace, CrmConstants.CRM_SFDC,
                    SourceCredentialType.PRODUCTION);
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_17004);
            assertTrue(e.getMessage().contains("Invalid"));
        }
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
    }
}
