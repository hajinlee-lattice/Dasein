package com.latticeengines.pls.service.impl;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.pls.service.CrmCredentialService;

public class CrmCredentialServiceImplDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    @Autowired
    private CrmCredentialService crmService;

    private String fullId;

    private static final String SFDC_PROD_USER = "apeters-widgettech@lattice-engines.com";
    private static final String SFDC_PROD_ORG_ID = "00D80000000KvZoEAK";
    private static final String SFDC_PROD_PASSWD = "Happy2010";
    private static final String SFDC_PROD_TOKEN = "oIogZVEFGbL3n0qiAp6F66TC";

    private static final String MKTO_USER = "latticeenginessandbox1_9026948050BD016F376AE6";
    private static final String MKTO_URL = "https://na-sj02.marketo.com/soap/mktows/2_0";
    private static final String MKTO_PASSSWD = "41802295835604145500BBDD0011770133777863CA58";

    private static final String ELQ_USER = "Matt.Sable";
    private static final String ELQ_PASSWORD = "Lattice2";
    private static final String ELQ_COMPANY = "TechnologyPartnerLatticeEngines";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        switchToSuperAdmin();
        fullId = testingTenants.get(0).getId();
    }

    @Test(groups = "deployment", enabled = false)
    public void verifyCredentialUsingDL() {
        // sfdc
        CrmCredential crmCredential = sfdcProductionCredentials();
        CrmCredential newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE,
                crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_PROD_ORG_ID);

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUrl(MKTO_URL);
        crmCredential.setUserName(MKTO_USER);
        crmCredential.setPassword(MKTO_PASSSWD);
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_MARKETO, fullId, null, crmCredential);
        Assert.assertNotNull(newCrmCredential);

        // eloqua
        crmCredential = new CrmCredential();
        crmCredential.setUserName(ELQ_USER);
        crmCredential.setPassword(ELQ_PASSWORD);
        crmCredential.setCompany(ELQ_COMPANY);
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_ELOQUA, fullId, null, crmCredential);
        Assert.assertNotNull(newCrmCredential);
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyCredentialUsingDL", enabled = false)
    public void getCredentialUsingDL() {
        CrmCredential newCrmCredential = crmService.getCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getOrgId(), SFDC_PROD_ORG_ID);
        Assert.assertEquals(newCrmCredential.getPassword(), SFDC_PROD_PASSWD);

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_MARKETO, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getUserName(), MKTO_USER);

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_ELOQUA, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getPassword(), ELQ_PASSWORD);
    }

    @Test(groups = "deployment", dependsOnMethods = "getCredentialUsingDL", enabled = false)
    public void removeCredentials() {
        crmService.removeCredentials(CrmConstants.CRM_SFDC, fullId, true);
        crmService.removeCredentials(CrmConstants.CRM_SFDC, fullId, false);
        crmService.removeCredentials(CrmConstants.CRM_MARKETO, fullId, true);
        crmService.removeCredentials(CrmConstants.CRM_ELOQUA, fullId, true);

        for (String crmType : Arrays.asList(CrmConstants.CRM_SFDC, CrmConstants.CRM_MARKETO, CrmConstants.CRM_ELOQUA)) {
            boolean exception = false;
            try {
                crmService.getCredential(crmType, fullId, Boolean.TRUE);
            } catch (LedpException e) {
                exception = true;
            }
            Assert.assertTrue(exception);

            exception = false;
            try {
                crmService.getCredential(crmType, fullId, Boolean.FALSE);
            } catch (LedpException e) {
                exception = true;
            }
            Assert.assertTrue(exception);
        }
    }

    private CrmCredential sfdcProductionCredentials() {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(SFDC_PROD_USER);
        crmCredential.setPassword(SFDC_PROD_PASSWD);
        crmCredential.setSecurityToken(SFDC_PROD_TOKEN);
        return crmCredential;
    }
}
