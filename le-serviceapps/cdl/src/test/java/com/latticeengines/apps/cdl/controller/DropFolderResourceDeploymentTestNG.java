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
        List<String> subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null, null);
        int defaultSize = subFolders.size();
        Assert.assertEquals(defaultSize, 5);

        dropBoxProxy.createTemplateFolder(mainCustomerSpace, "FirstSystem", null, null);
        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null, null);
        defaultSize = subFolders.size();
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, null,
                    BusinessEntity.Account.name(), null);
    }
}
