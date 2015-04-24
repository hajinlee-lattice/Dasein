package com.latticeengines.pls.controller;

import org.apache.commons.codec.digest.DigestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
                + "/pls/verifycrm/sfdc?tenantId=tenantId&contractId=contractI", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "verifySfdcCredential" })
    public void getSfdcCredential() {
        CrmCredential crmCredential = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/verifycrm/sfdc?tenantId=tenantId&contractId=contractId", CrmCredential.class);
        Assert.assertEquals(crmCredential.getOrgId(), "00D80000000KvZoEAK");
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
                + "/pls/verifycrm/marketo?tenantId=tenantId&contractId=contractI", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "latticeenginessandbox1_9026948050BD016F376AE6");
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
                + "/pls/verifycrm/eloqua?tenantId=tenantId&contractId=contractI", crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getUserName(), "Matt.Sable");
    }

}
