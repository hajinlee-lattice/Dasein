package com.latticeengines.pls.service.impl;

import junit.framework.Assert;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmCredentialService;

public class CrmCredentialServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmCredentialService crmService;

    @Test(groups = "functional")
    public void verifyCredential() {

        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = crmService.verifyCredential("sfdc", "tenantId", "contractId", crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        newCrmCredential = crmService.verifyCredential("marketo", "tenantId", "contractId", crmCredential);
        
        // eloqua
        crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice1");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        newCrmCredential = crmService.verifyCredential("eloqua", "tenantId", "contractId", crmCredential);

    }

    @Test(groups = "functional", dependsOnMethods = "verifyCredential")
    public void getCredential() {
        CrmCredentialService crmService = new CrmCredentialServiceImpl();
        CrmCredential newCrmCredential = crmService.getCredential("sfdc", "tenantId", "contractId");
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

    }
}
