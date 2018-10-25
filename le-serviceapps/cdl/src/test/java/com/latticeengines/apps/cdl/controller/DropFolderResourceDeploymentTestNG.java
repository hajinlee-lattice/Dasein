package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class DropFolderResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private DropBoxProxy dropBoxProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void test() {
        List<String> subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null);
        int defaultSize = subFolders.size();
        Assert.assertTrue(defaultSize > 0);

        dropBoxProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template1");
        dropBoxProxy.createTemplateFolder(mainTestTenant.getName(), BusinessEntity.Account.name(), "template2");
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template3");
        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), null);
        Assert.assertEquals(subFolders.size(), 3);

        dropBoxProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template1/test1/test2");
        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), "template1");
        Assert.assertEquals(subFolders.size(), 1);

        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), "template1/test1");
        Assert.assertEquals(subFolders.size(), 1);

        dropBoxProxy.createTemplateFolder(mainCustomerSpace, "Account123", "template1");
        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null);
        Assert.assertEquals(subFolders.size(), defaultSize + 2);

        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, "Account123", null);
        Assert.assertEquals(subFolders.size(), 1);
    }
}
