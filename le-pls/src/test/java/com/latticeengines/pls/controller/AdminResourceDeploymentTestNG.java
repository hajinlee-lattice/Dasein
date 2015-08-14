package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class AdminResourceDeploymentTestNG extends PlsFunctionalTestNGBase{

    @Autowired
    private AdminResourceTestNG adminResourceTestNG;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        adminResourceTestNG.setup();
        turnOffSslChecking();
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() {
        adminResourceTestNG.teardown();
    }

    @Test(groups = "deployment")
    public void addAdminUser() {
        adminResourceTestNG.addAdminUser();
    }

}
