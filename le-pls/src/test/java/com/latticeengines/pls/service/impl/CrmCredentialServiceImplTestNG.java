package com.latticeengines.pls.service.impl;

import java.util.Arrays;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmCredentialService;

public class CrmCredentialServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmCredentialService crmService;

    private final String contractId = "PLSCrmConfig";
    private final String tenantId = "PLSCrmConfig";
    private final String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;
    private final String fullId = String.format("%s.%s.%s", contractId, tenantId, spaceId);

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                contractId, tenantId, spaceId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            //  ignore
        }
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                contractId, tenantId, spaceId);
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void verifyCredential() {
        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId,
                Boolean.TRUE, crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // beware that password might change for this sandbox user
        crmCredential = new CrmCredential();
        crmCredential.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE,
                crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00DM0000001dg3uMAA");

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_MARKETO, fullId, null, crmCredential);
        Assert.assertNotNull(newCrmCredential);

        // eloqua
        crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice2");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_ELOQUA, fullId, null, crmCredential);
        Assert.assertNotNull(newCrmCredential);
    }

    @Test(groups = "functional")
    public void verifyCredentialWrongPassword() {
        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("nope");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        boolean encounteredException = false;
        try {
             crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE, crmCredential);
        } catch (Exception e) {
            encounteredException = true;
        }
        Assert.assertTrue(encounteredException, "Wrong password should cause exception while validating sfdc.");

        crmCredential = new CrmCredential();
        crmCredential.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        crmCredential.setPassword("nope");
        crmCredential.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        encounteredException = false;
        try {
            crmService.verifyCredential(CrmConstants.CRM_SFDC, fullId, Boolean.FALSE, crmCredential);
        } catch (Exception e) {
            encounteredException = true;
        }
        Assert.assertTrue(encounteredException, "Wrong password should cause exception while validating sfdcsandbox.");
    }

    @Test(groups = "functional", dependsOnMethods = "verifyCredential")
    public void getCredential() {
        CrmCredential newCrmCredential = crmService.getCredential(CrmConstants.CRM_SFDC, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_MARKETO, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_ELOQUA, fullId, Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getPassword(), "Lattice2");
    }

    @Test(groups = "functional", dependsOnMethods = "getCredential")
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

}
