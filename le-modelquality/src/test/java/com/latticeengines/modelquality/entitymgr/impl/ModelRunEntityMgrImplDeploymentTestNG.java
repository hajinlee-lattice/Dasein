package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class ModelRunEntityMgrImplDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private ModelRun modelRun;
    private final String modelRunName = "ModelRunEntityMgrImplDeploymentTestNG";

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        ModelRun alreadyExists = modelRunEntityMgr.findByName(modelRunName);
        if (alreadyExists != null)
            modelRunEntityMgr.delete(alreadyExists);
        super.setup();
        modelRun = new ModelRun();
        modelRun.setName(modelRunName);
        modelRun.setDescription("Test pipeline for persistence.");
        modelRun.setCreated(new Date());
        modelRun.setStatus(ModelRunStatus.NEW);
        modelRun.setAnalyticPipeline(analyticPipelines.get(0));
        modelRun.setDataSet(dataset);
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        modelRunEntityMgr.delete(modelRun);
        super.tearDown();
    }

    @Test(groups = "deployment")
    public void create() throws Exception {
        modelRunEntityMgr.create(modelRun);

        List<ModelRun> retrievedModelRuns = modelRunEntityMgr.findAll();
        assertNotNull(retrievedModelRuns);
        ModelRun retrievedModelRun = modelRunEntityMgr.findByName(modelRunName);
        assertEquals(retrievedModelRun.getName(), modelRun.getName());
        assertEquals(retrievedModelRun.getDescription(), modelRun.getDescription());
        assertEquals(retrievedModelRun.getStatus(), modelRun.getStatus());

        SelectedConfig selectedConfig = new SelectedConfig();
        AnalyticPipeline analyticPipeline = retrievedModelRun.getAnalyticPipeline();
        DataSet dataset = retrievedModelRun.getDataSet();
        selectedConfig.setPipeline(analyticPipeline.getPipeline());
        selectedConfig.setAlgorithm(analyticPipeline.getAlgorithm());
        selectedConfig.setDataSet(dataset);
        selectedConfig.setPropData(analyticPipeline.getPropData());
        selectedConfig.setDataFlow(analyticPipeline.getDataFlow());
        selectedConfig.setSampling(analyticPipeline.getSampling());
        System.out.println(JsonUtils.serialize(selectedConfig));
    }
}
