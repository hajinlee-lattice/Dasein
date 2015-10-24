package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class EaiCredentialValidationServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private EaiCredentialValidationService eaiCredentialValidationService;

    @Value("${eai.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.salesforce.password}")
    private String salesforcePasswd;

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
    public void testVerifyCrmCredential() {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
        eaiCredentialValidationService.validateCrmCredential(customerSpace);
    }

    @Test(groups = "functional")
    public void testVerifyBadCrmCredential() {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("some bad username");
        crmCredential.setPassword("some bad password");
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
        try {
            eaiCredentialValidationService.validateCrmCredential(customerSpace);
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_17004);
            assertTrue(e.getMessage().contains("Invalid"));
        }
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        crmCredentialZKService.removeCredentials(customer, customer, true);
    }
}
