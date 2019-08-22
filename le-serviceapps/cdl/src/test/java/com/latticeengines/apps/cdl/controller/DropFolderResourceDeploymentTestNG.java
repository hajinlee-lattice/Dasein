package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class DropFolderResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final String TEST_SYSTEM_NAME = "FirstSystem";

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void test() {
        List<String> subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null, null);
        int defaultSize = subFolders.size();
        Assert.assertEquals(defaultSize, 5);

        // create import system
        S3ImportSystem sys = new S3ImportSystem();
        sys.setName(TEST_SYSTEM_NAME);
        sys.setDisplayName(TEST_SYSTEM_NAME);
        sys.setPriority(1);
        sys.setSystemType(S3ImportSystem.SystemType.Other);
        sys.setTenant(mainTestTenant);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, sys);

        // create drop folder
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, TEST_SYSTEM_NAME, null, null);
        subFolders = dropBoxProxy.getAllSubFolders(mainCustomerSpace, null, null, null);
        defaultSize = subFolders.size();
        dropBoxProxy.createTemplateFolder(mainCustomerSpace, null,
                    BusinessEntity.Account.name(), null);
        // TODO add assertion on result
    }
}
