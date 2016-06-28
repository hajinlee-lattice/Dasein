package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class PipelineEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private Pipeline pipeline;

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private PipelineStepEntityMgr pipelineStepEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        List<Pipeline> oldPipelines = pipelineEntityMgr.findAll();
        pipelineEntityMgr.deletePipelines(oldPipelines);
        
        pipeline = new Pipeline();
        pipeline.setName("Pipeline1");

        PipelineStep step = new PipelineStep();
        step.setName("StepName1");
        step.setScript("StepScript1");
        List<PipelinePropertyDef> pipelinePropertyDefs = new ArrayList<>();
        PipelinePropertyDef pipelinePropertyDef = new PipelinePropertyDef();
        pipelinePropertyDef.setName("StepDef1");

        PipelinePropertyValue pipeLinePropertyValue = new PipelinePropertyValue();
        pipeLinePropertyValue.setValue("PropertyValue1");
        pipeLinePropertyValue.setPipelinePropertyDef(pipelinePropertyDef);
        pipelinePropertyDef.addPipelinePropertyValue(pipeLinePropertyValue);

        pipelinePropertyDefs.add(pipelinePropertyDef);
        step.addPipelinePropertyDef(pipelinePropertyDef);

        pipeline.addPipelineStep(step);
    }

    @Test(groups = "functional")
    public void create() {

        pipelineEntityMgr.create(pipeline);

        List<Pipeline> pipelines = pipelineEntityMgr.findAll();
        assertEquals(pipelines.size(), 1);
        Pipeline retrievedPineline = pipelines.get(0);

        assertEquals(retrievedPineline.getName(), pipeline.getName());
        assertEquals(retrievedPineline.getPipelineSteps().size(), pipeline.getPipelineSteps().size());

        PipelineStep pipelineStep = pipeline.getPipelineSteps().get(0);
        PipelineStep retrievedPipelineStep = retrievedPineline.getPipelineSteps().get(0);

        assertEquals(retrievedPipelineStep.getName(), pipelineStep.getName());
        assertEquals(retrievedPipelineStep.getScript(), pipelineStep.getScript());

        assertEquals(retrievedPipelineStep.getPipelinePropertyDefs().size(), 1);
        PipelinePropertyDef retrievedPipelinePropertyDef = retrievedPipelineStep.getPipelinePropertyDefs().get(0);
        PipelinePropertyDef pipelinePropertyDef = pipelineStep.getPipelinePropertyDefs().get(0);

        assertEquals(retrievedPipelinePropertyDef.getName(), pipelinePropertyDef.getName());
        assertEquals(retrievedPipelinePropertyDef.getPipelinePropertyValues().size(), pipelinePropertyDef
                .getPipelinePropertyValues().size());
        assertEquals(retrievedPipelinePropertyDef.getPipelinePropertyValues().get(0).getValue(), pipelinePropertyDef
                .getPipelinePropertyValues().get(0).getValue());

    }
}
