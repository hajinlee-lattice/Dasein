package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DropFolderProxy;

public class DropFolderResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private DropFolderProxy dropFolderProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void test() {
        List<String> subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, null, null);
        int defaultSize = subFolders.size();
        Assert.assertTrue(defaultSize > 0);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template1");
        dropFolderProxy.createTemplateFolder(mainTestTenant.getName(), BusinessEntity.Account.name(), "template2");
        dropFolderProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template3");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), null);
        Assert.assertEquals(subFolders.size(), 3);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, BusinessEntity.Account.name(), "template1/test1/test2");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), "template1");
        Assert.assertEquals(subFolders.size(), 1);

        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, BusinessEntity.Account.name(), "template1/test1");
        Assert.assertEquals(subFolders.size(), 1);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, "Account123", "template1");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, null, null);
        Assert.assertEquals(subFolders.size(), defaultSize + 2);

        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, "Account123", null);
        Assert.assertEquals(subFolders.size(), 1);
    }
}
