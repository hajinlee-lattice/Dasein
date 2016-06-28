package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class ModelConfigEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    @Autowired
    private ModelConfigEntityMgr modelConfigEntityMgr;
    
    private ModelConfig modelConfig;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelConfigEntityMgr.deleteAll();
        modelConfig = new ModelConfig();
        SelectedConfig config = new SelectedConfig();
        config.setPipeline(createLocalPipeline());
        
        modelConfig.setDescription("Test pipeline for persistence.");
        modelConfig.setCreated(new Date());
        modelConfig.setSelectedConfig(config);
        modelConfig.setName("ModelConfig1");
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
        modelConfigEntityMgr.create(modelConfig);
        
        List<ModelConfig> retrievedModelConfigs = modelConfigEntityMgr.findAll();
        assertEquals(retrievedModelConfigs.size(), 1);
        ModelConfig retrievedModelConfig = retrievedModelConfigs.get(0);
        assertEquals(retrievedModelConfig.getName(), modelConfig.getName());
        assertEquals(retrievedModelConfig.getDescription(), modelConfig.getDescription());
        
        SelectedConfig config = retrievedModelConfig.getSelectedConfig();
        System.out.println(JsonUtils.serialize(config));
    }
}
