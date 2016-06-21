package com.latticeengines.pls.controller;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class ModelResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA);
    }
    
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%s", mainTestTenant.getName()));
    }

    @Test(groups = "deployment")
    public void modelForPmml() {
    }
}
