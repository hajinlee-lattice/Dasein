package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class SalesforceURLEntityMgrImplTestNG extends PlsFunctionalTestNGBaseDeprecated {
    @Autowired
    private SalesforceURLEntityMgr salesforceURLEntityMgr;

    @Test(groups = "functional")
    public void findByURLName() {
        String name = "FunctionalTestURL";
        String url = "https://login.salesforce.com/";

        SalesforceURL sfdcURLCreated = new SalesforceURL();
        sfdcURLCreated.setName(name);
        sfdcURLCreated.setURL(url);
        Assert.assertEquals(sfdcURLCreated.getName(), name);
        Assert.assertEquals(sfdcURLCreated.getURL(), url);
        salesforceURLEntityMgr.create(sfdcURLCreated);

        SalesforceURL urlFound = salesforceURLEntityMgr.findByURLName(name);
        Assert.assertNotNull(urlFound);
        Assert.assertEquals(urlFound.getPid(), sfdcURLCreated.getPid());
        Assert.assertEquals(urlFound.getName(), sfdcURLCreated.getName());
        Assert.assertEquals(urlFound.getURL(), sfdcURLCreated.getURL());

        salesforceURLEntityMgr.delete(sfdcURLCreated);
    }

    @Test(groups = "functional")
    public void findByURLNameNotFound() {
        SalesforceURL url = salesforceURLEntityMgr.findByURLName("URL_NotExist_!@#$%");
        Assert.assertNull(url);
    }
}