package com.latticeengines.eai.service.impl.salesforce.strategy;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.camel.CamelContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.SalesforceExtractAndImportUtil;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class SalesforceImplStrategyTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private SalesforceImportStrategyBase salesforceImportStrategyBase;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private ImportContext importContext;

    private String customer = "SFDC-Eai-ImportMetadata-Customer";

    @Value("${eai.test.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.test.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.test.salesforce.securitytoken}")
    private String salesforceSecurityToken;

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        initZK(customer);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
    }

    @Test(groups = "functional")
    public void testValidateMetadataImport() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();

        Table account = SalesforceExtractAndImportUtil.createAccountWithNonExistingAttr();
        boolean exception = false;
        try {
            salesforceImportStrategyBase.importMetadata(camelContext.createProducerTemplate(), account, "",
                    importContext);
        } catch (LedpException e) {
            exception = true;
            assertEquals(e.getCode(), LedpCode.LEDP_17003);
            assertTrue(e.getMessage().contains("IsConverted, Company, NaicsCode, Salutation, Status"));
        }

        assertTrue(exception, "Exception should have been thrown.");
    }

}
