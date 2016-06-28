package com.latticeengines.modelquality.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class FileModelRunSerivceImplTestNG extends ModelQualityDeploymentTestNGBase {

    private ModelRun modelRun1;
    private ModelRun modelRun2;

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        cleanup();
        modelRun1 = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
        modelRun2 = createModelRun(AlgorithmFactory.ALGORITHM_NAME_LR);
    }

    @Test(groups = "deployment")
    public void run() {
        try {
            modelRunService.run(modelRun1);
            System.out.println("Finished modelRun1");

            modelRunService.run(modelRun2);
            System.out.println("Finished modelRun2");

        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
