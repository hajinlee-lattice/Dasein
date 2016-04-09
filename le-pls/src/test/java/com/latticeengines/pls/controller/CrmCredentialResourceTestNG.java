package com.latticeengines.pls.controller;

import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class CrmCredentialResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {
    @BeforeClass(groups = { "deployment" }, enabled = false)
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        try {
            camille.delete(path);
        } catch (Exception ex) {
            //ignore
        }
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
        turnOffSslChecking();
    }

    @AfterClass(groups = { "deployment" }, enabled = false)
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), "contractId");
        camille.delete(path);
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void verifySfdcCredential() {
        switchToSuperAdmin();
        restTemplate.setErrorHandler(statusErrorHandler);

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=contractId.tenantId.spaceId&isProduction=true&verifyOnly=true", crmCredential,
                CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifySfdcCredential" }, enabled = false)
    public void getSfdcCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=contractId.tenantId.spaceId&&isProduction=true", CrmCredential.class);
        Assert.assertEquals(crmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(crmCredential.getUserName(), "apeters-widgettech@lattice-engines.com");
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void verifyMarketoCredential() {
        switchToSuperAdmin();
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/marketo?tenantId=contractId.tenantId.spaceId&verifyOnly=true", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifyMarketoCredential" }, enabled = false)
    public void getMarketoCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/marketo?tenantId=contractId.tenantId.spaceId", CrmCredential.class);
        Assert.assertEquals(crmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");
        Assert.assertEquals(crmCredential.getPassword(), "41802295835604145500BBDD0011770133777863CA58");
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void verifyEloquaCredential() {
        switchToSuperAdmin();
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice1");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/eloqua?tenantId=contractId.tenantId.spaceId&verifyOnly=true", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "Matt.Sable");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifyEloquaCredential" }, enabled = false)
    public void getEloquaCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/eloqua?tenantId=contractId.tenantId.spaceId", CrmCredential.class);
        Assert.assertEquals(crmCredential.getUserName(), "Matt.Sable");
        Assert.assertEquals(crmCredential.getPassword(), "Lattice1");
    }

}
