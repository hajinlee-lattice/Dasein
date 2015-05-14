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
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmConstants;
import com.latticeengines.pls.service.CrmCredentialService;

public class CrmCredentialServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmCredentialService crmService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
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
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void verifyCredential() {

        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, "contractId.tenantId.spaceId",
                Boolean.TRUE, crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_SFDC, "contractId.tenantId.spaceId", Boolean.FALSE,
                crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_MARKETO, "contractId.tenantId.spaceId", null, crmCredential);
        Assert.assertNotNull(newCrmCredential);

        // eloqua
        crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice1");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        newCrmCredential = crmService.verifyCredential(CrmConstants.CRM_ELOQUA, "contractId.tenantId.spaceId", null, crmCredential);
        Assert.assertNotNull(newCrmCredential);
    }

    @Test(groups = "functional", dependsOnMethods = "verifyCredential")
    public void getCredential() {
        CrmCredentialService crmService = new CrmCredentialServiceImpl();
        CrmCredential newCrmCredential = crmService.getCredential(CrmConstants.CRM_SFDC, "contractId.tenantId.spaceId", Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_MARKETO, "contractId.tenantId.spaceId", Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");

        newCrmCredential = crmService.getCredential(CrmConstants.CRM_ELOQUA, "contractId.tenantId.spaceId", Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getPassword(), "Lattice1");
    }

    @Test(groups = "functional", dependsOnMethods = "getCredential")
    public void removeCredentials() {
        CrmCredentialService crmService = new CrmCredentialServiceImpl();
        crmService.removeCredentials(CrmConstants.CRM_SFDC, "contractId.tenantId.spaceId", true);
        crmService.removeCredentials(CrmConstants.CRM_SFDC, "contractId.tenantId.spaceId", false);
        crmService.removeCredentials(CrmConstants.CRM_MARKETO, "contractId.tenantId.spaceId", true);
        crmService.removeCredentials(CrmConstants.CRM_ELOQUA, "contractId.tenantId.spaceId", true);

        for (String crmType : Arrays.asList(CrmConstants.CRM_SFDC, CrmConstants.CRM_MARKETO, CrmConstants.CRM_ELOQUA)) {
            boolean exception = false;
            try {
                crmService.getCredential(crmType, "contractId.tenantId.spaceId", Boolean.TRUE);
            } catch (LedpException e) {
                exception = true;
            }
            Assert.assertTrue(exception);

            exception = false;
            try {
                crmService.getCredential(crmType, "contractId.tenantId.spaceId", Boolean.FALSE);
            } catch (LedpException e) {
                exception = true;
            }
            Assert.assertTrue(exception);
        }
    }

}
