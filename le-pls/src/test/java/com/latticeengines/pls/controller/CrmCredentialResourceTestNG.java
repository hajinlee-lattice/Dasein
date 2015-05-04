package com.latticeengines.pls.controller;

import org.apache.commons.codec.digest.DigestUtils;
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
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class CrmCredentialResourceTestNG extends PlsFunctionalTestNGBase {

    private Ticket ticket = null;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupUsers();
        ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        setupDb(tenant1, tenant2);

        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        try {
            camille.delete(path);
        } catch (Exception ex) {
        }
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
    }

    @AfterClass(groups = { "deployment" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId", "spaceId");
        camille.delete(path);
    }

    @Test(groups = { "deployment" })
    public void verifySfdcCredential() {
        UserDocument adminDoc = loginAndAttachAdmin();
        useSessionDoc(adminDoc);
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=contractId.tenantId.spaceId&isProduction=true", crmCredential,
                CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifySfdcCredential" })
    public void getSfdcCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=contractId.tenantId.spaceId&&isProduction=true", CrmCredential.class);
        Assert.assertEquals(crmCredential.getOrgId(), "00D80000000KvZoEAK");
        Assert.assertEquals(crmCredential.getUserName(), "apeters-widgettech@lattice-engines.com");
    }

    @Test(groups = { "deployment" })
    public void verifyMarketoCredential() {
        UserDocument adminDoc = loginAndAttachAdmin();
        useSessionDoc(adminDoc);
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/marketo?tenantId=contractId.tenantId.spaceId", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifyMarketoCredential" })
    public void getMarketoCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/marketo?tenantId=contractId.tenantId.spaceId", CrmCredential.class);
        Assert.assertEquals(crmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");
        Assert.assertEquals(crmCredential.getPassword(), "41802295835604145500BBDD0011770133777863CA58");
    }

    @Test(groups = { "deployment" })
    public void verifyEloquaCredential() {
        UserDocument adminDoc = loginAndAttachAdmin();
        useSessionDoc(adminDoc);
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("Matt.Sable");
        crmCredential.setPassword("Lattice1");
        crmCredential.setCompany("TechnologyPartnerLatticeEngines");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/eloqua?tenantId=contractId.tenantId.spaceId", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "Matt.Sable");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifyEloquaCredential" })
    public void getEloquaCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/credentials/eloqua?tenantId=contractId.tenantId.spaceId", CrmCredential.class);
        Assert.assertEquals(crmCredential.getUserName(), "Matt.Sable");
        Assert.assertEquals(crmCredential.getPassword(), "Lattice1");
    }

}
