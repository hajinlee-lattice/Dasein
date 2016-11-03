package com.latticeengines.modelquality.service.impl;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class FileModelRunServiceImplDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private ModelRun modelRun1;
    private ModelRun modelRun2;

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        super.cleanupDb();
        super.cleanupHdfs();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null);

        List<ModelRun> modelRuns = createModelRuns();
        modelRun1 = modelRuns.get(0);
        modelRun2 = modelRuns.get(1);
    }

    @Test(groups = "manual")
    public void run() {
        try {
            modelRunService.createModelRun(new ModelRunEntityNames(modelRun1), null);
            System.out.println("Finished modelRun1");

            modelRunService.createModelRun(new ModelRunEntityNames(modelRun2), null);
            System.out.println("Finished modelRun2");

        } catch (Exception ex) {
            System.out.println(ExceptionUtils.getFullStackTrace(ex));
            Assert.fail(ex.getMessage());
        }
    }
}
