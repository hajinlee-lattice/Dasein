package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class PipelineResourceTestNG extends ModelQualityDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void upsertPipelines() {
        try {
            Pipeline pipeline1 = createPipeline(1);
            Pipeline pipeline2 = createPipeline(2);
            pipeline2.getPipelineSteps().get(0).addPipeline(pipeline1);
            pipeline1.addPipelineStep(pipeline2.getPipelineSteps().get(0));
            
            ResponseDocument<String> response = modelQualityProxy.upsertPipelines(Arrays.asList(pipeline1, pipeline2));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertPipelines")
    public void getPipelines() {
        try {
            ResponseDocument<List<Pipeline>> response = modelQualityProxy.getPipelines();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 2);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
