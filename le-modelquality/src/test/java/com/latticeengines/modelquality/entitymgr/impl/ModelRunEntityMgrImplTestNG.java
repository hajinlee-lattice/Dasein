package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class ModelRunEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;
    
    private ModelRun modelRun;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelRunEntityMgr.deleteAll();
        modelRun = new ModelRun();
        SelectedConfig config = new SelectedConfig();
        config.setPipeline(createLocalPipeline());
        
        modelRun.setDescription("Test pipeline for persistence.");
        modelRun.setCreated(new Date());
        modelRun.setSelectedConfig(config);
        modelRun.setName("ModelRun1");
        modelRun.setStatus(ModelRunStatus.NEW);;
    }
    
    private Pipeline createLocalPipeline() {
        Pipeline pipeline = new Pipeline();
        pipeline.setName("Test pipeline");
        PipelineStep pipelineStep = new PipelineStep();
        pipelineStep.setName("pivotstep");
        pipelineStep.setScript("pivotstep.py");
        PipelinePropertyDef pipelinePropertyDef = new PipelinePropertyDef("pivotstep.threshold");
        PipelinePropertyValue pipelinePropertyValue = new PipelinePropertyValue("0.10");
        
        pipeline.addPipelineStep(pipelineStep);
        pipelineStep.addPipelinePropertyDef(pipelinePropertyDef);
        pipelinePropertyDef.addPipelinePropertyValue(pipelinePropertyValue);
        
        return pipeline;
    }
    
    @Test(groups = "functional")
    public void create() throws Exception {
        modelRunEntityMgr.create(modelRun);
        
        List<ModelRun> retrievedModelRuns = modelRunEntityMgr.findAll();
        assertEquals(retrievedModelRuns.size(), 1);
        ModelRun retrievedModelRun = retrievedModelRuns.get(0);
        assertEquals(retrievedModelRun.getName(), modelRun.getName());
        assertEquals(retrievedModelRun.getDescription(), modelRun.getDescription());
        assertEquals(retrievedModelRun.getStatus(), modelRun.getStatus());
        
        
        SelectedConfig config = retrievedModelRun.getSelectedConfig();
        System.out.println(JsonUtils.serialize(config));
    }
}
