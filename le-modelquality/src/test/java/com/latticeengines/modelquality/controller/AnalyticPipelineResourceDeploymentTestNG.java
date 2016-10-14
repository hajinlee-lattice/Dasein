package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class AnalyticPipelineResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
    }

    @Test(groups = "deployment")
    public void createAnalyticPipelineFromProduction() {
        AnalyticPipelineEntityNames analyticPipeline = modelQualityProxy.createAnalyticPipelineFromProduction();
        Assert.assertNotNull(analyticPipeline);
    }

    @Test(groups = "deployment", dependsOnMethods = "createAnalyticPipelineFromProduction")
    public void getAnalyticPipelines() {
        List<AnalyticPipelineEntityNames> algorithms = modelQualityProxy.getAnalyticPipelines();
        Assert.assertEquals(algorithms.size(), 1);
    }
}
