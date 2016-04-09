package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.SalesforceURLConstants;
import com.latticeengines.pls.service.SalesforceURLService;

public class SalesforceURLServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {
    private SalesforceURL sfdcURLLPCreated;
    private SalesforceURL sfdcURLAPCreated;

    @Autowired
    private SalesforceURLService salesforceURLService;

    @Autowired
    private SalesforceURLEntityMgr salesforceURLEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        // If there is no BisLP/BisAP in SALESFORCE_URL table, insert them
        SalesforceURL sfdcURLLP = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        if (sfdcURLLP == null) {
            sfdcURLLPCreated = createSalesforceURL(SalesforceURLConstants.BISLP_NAME, BISLP_URL);
        }

        SalesforceURL sfdcURLAP = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        if (sfdcURLAP == null) {
            sfdcURLAPCreated = createSalesforceURL(SalesforceURLConstants.BISAP_NAME, BISAP_URL);
        }
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        // Delete records that inserted in setup()
        if (sfdcURLLPCreated != null) {
            salesforceURLEntityMgr.delete(sfdcURLLPCreated);
        }

        if (sfdcURLAPCreated != null) {
            salesforceURLEntityMgr.delete(sfdcURLAPCreated);
        }
    }

    @Test(groups = "functional")
    public void getBisLP() {
        String url = salesforceURLService.getBisLP();
        Assert.assertNotNull(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        Assert.assertNotNull(sfdcURL);

        String sfdcURLLP = sfdcURL.getURL();
        Assert.assertEquals(url, sfdcURLLP);
    }

    @Test(groups = "functional")
    public void getBisLPSandbox() {
        String url = salesforceURLService.getBisLPSandbox();
        Assert.assertNotNull(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        Assert.assertNotNull(sfdcURL);

        String sfdcURLLP = sfdcURL.getURL();
        String sfdcURLLPSandbox = salesforceURLService.getSandboxURL(sfdcURLLP);
        Assert.assertEquals(url, sfdcURLLPSandbox);
    }

    @Test(groups = "functional")
    public void getBisAP() {
        String url = salesforceURLService.getBisAP();
        Assert.assertNotNull(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        Assert.assertNotNull(sfdcURL);

        String sfdcURLAP = sfdcURL.getURL();
        Assert.assertEquals(url, sfdcURLAP);
    }

    @Test(groups = "functional")
    public void getBisAPSandbox() {
        String url = salesforceURLService.getBisAPSandbox();
        Assert.assertNotNull(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        Assert.assertNotNull(sfdcURL);

        String sfdcURLAP = sfdcURL.getURL();
        String sfdcURLAPSandbox = salesforceURLService.getSandboxURL(sfdcURLAP);
        Assert.assertEquals(url, sfdcURLAPSandbox);
    }

    @Test(groups = "functional")
    public void getSanboxURL(){
        String url = "https://login.salesforce.com/";
        String sandboxURL = salesforceURLService.getSandboxURL(url);
        String sandboxURLExpected = url.replaceAll("login.", "test.");
        Assert.assertEquals(sandboxURL, sandboxURLExpected);
    }

    private SalesforceURL createSalesforceURL(String name, String url) {
        SalesforceURL sfdcURL = new SalesforceURL();
        sfdcURL.setName(name);
        sfdcURL.setURL(url);
        salesforceURLEntityMgr.create(sfdcURL);
        return sfdcURL;
    }

}