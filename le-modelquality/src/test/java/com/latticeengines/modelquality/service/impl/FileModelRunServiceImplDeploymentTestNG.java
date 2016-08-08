package com.latticeengines.modelquality.service.impl;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class FileModelRunServiceImplDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private ModelRun modelRun1;
    private ModelRun modelRun2;

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        cleanup();
        modelRun1 = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
        modelRun2 = createModelRun(AlgorithmFactory.ALGORITHM_NAME_LR);
    }

    @Test(groups = "manual")
    public void run() {
        try {
            modelRunService.run(modelRun1, null);
            System.out.println("Finished modelRun1");

            modelRunService.run(modelRun2, null);
            System.out.println("Finished modelRun2");

        } catch (Exception ex) {
            System.out.println(ExceptionUtils.getFullStackTrace(ex));
            Assert.fail(ex.getMessage());
        }
    }
}
