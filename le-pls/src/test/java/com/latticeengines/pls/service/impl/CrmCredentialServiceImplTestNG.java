package com.latticeengines.pls.service.impl;

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
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmCredentialService;

public class CrmCredentialServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmCredentialService crmService;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), "contractId", "tenantId");
        try {
            camille.delete(path);
        } catch (Exception ex) {
        }
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
    }

    @AfterClass(groups = { "deployment" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), "contractId", "tenantId");
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void verifyCredential() {

        // sfdc
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = crmService.verifyCredential("sfdc", "contractId.tenantId.displayName",
                Boolean.TRUE, crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        newCrmCredential = crmService.verifyCredential("sfdc", "contractId.tenantId.displayName", Boolean.FALSE,
                crmCredential);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        newCrmCredential = crmService.verifyCredential("marketo", "contractId.tenantId.displayName", null,
                crmCredential);

        // eloqua
        crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice1");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        newCrmCredential = crmService
                .verifyCredential("eloqua", "contractId.tenantId.displayName", null, crmCredential);

    }

    @Test(groups = "functional", dependsOnMethods = "verifyCredential")
    public void getSFDCCredential() {
        CrmCredentialService crmService = new CrmCredentialServiceImpl();
        CrmCredential newCrmCredential = crmService.getCredential("sfdc", "contractId.tenantId.displayName",
                Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");

        newCrmCredential = crmService.getCredential("marketo", "contractId.tenantId.displayName", Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");

        newCrmCredential = crmService.getCredential("eloqua", "contractId.tenantId.displayName", Boolean.TRUE);
        Assert.assertEquals(newCrmCredential.getPassword(), "Lattice1");
    }

}
