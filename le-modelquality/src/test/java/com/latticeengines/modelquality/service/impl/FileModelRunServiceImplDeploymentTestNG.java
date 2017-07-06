package com.latticeengines.modelquality.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class FileModelRunServiceImplDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @Override
    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        super.setup();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null, null);
    }

    @Test(groups = "manual")
    public void run() {
        try {
            modelRunService.createModelRun(modelRunEntityNames.get(0), null);
            ModelRun modelRun = modelRunEntityMgr.findByName(modelRunEntityNames.get(0).getName());
            modelRunEntityMgr.delete(modelRun);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
