package com.latticeengines.apps.cdl.controller;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.DropFolderProxy;

public class DropFolderResourceDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DropFolderResourceDeploymentTestNG.class);

    @Inject
    private DropFolderProxy dropFolderProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void test() {
        List<String> subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, null, null);
        Assert.assertEquals(subFolders.size(), 5);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, "Accounts", "template1");
        dropFolderProxy.createTemplateFolder(mainTestTenant.getName(), "/Accounts", "template2");
        dropFolderProxy.createTemplateFolder(mainCustomerSpace, "Accounts/", "template3");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, "/Accounts/", null);
        Assert.assertEquals(subFolders.size(), 3);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, "/Accounts", "template1/test1/test2");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, "Accounts", "template1");
        Assert.assertEquals(subFolders.size(), 1);

        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, "Accounts", "template1/test1");
        Assert.assertEquals(subFolders.size(), 1);

        dropFolderProxy.createTemplateFolder(mainCustomerSpace, "Account123", "template1");
        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, null, null);
        Assert.assertEquals(subFolders.size(), 6);

        subFolders = dropFolderProxy.getAllSubFolders(mainCustomerSpace, "Account123", null);
        Assert.assertEquals(subFolders.size(), 1);
    }
}
