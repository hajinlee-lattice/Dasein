package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class PipelineEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private Pipeline pipeline;
    private final String pipelineName = "PipelineEntityMgrImplTestNG";
    private PipelineStep step;
    private final String pipelineStepName = "PipelineEntityMgrImplTestNG-Step";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        Pipeline alreadyExists = pipelineEntityMgr.findByName(pipelineName);
        if (alreadyExists != null)
            pipelineEntityMgr.delete(alreadyExists);

        pipeline = new Pipeline();
        pipeline.setName(pipelineName);
        pipeline.setDescription("Test Pipeline");

        step = new PipelineStep();
        step.setName(pipelineStepName);
        step.setScript("StepScript1");
        step.setMainClassName("StepName1");
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

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        pipelineEntityMgr.delete(pipeline);
        step.setPipelines(Collections.emptyList());
        pipelineStepEntityMgr.delete(step);
        super.tearDown();
    }

    @Test(groups = "functional")
    public void create() {
        pipelineEntityMgr.create(pipeline);

        List<Pipeline> pipelines = pipelineEntityMgr.findAll();
        assertNotNull(pipelines);

        Pipeline retrievedPineline = pipelineEntityMgr.findByName(pipelineName);
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
        assertEquals(retrievedPipelinePropertyDef.getPipelinePropertyValues().size(), //
                pipelinePropertyDef.getPipelinePropertyValues().size());
        assertEquals(retrievedPipelinePropertyDef.getPipelinePropertyValues().get(0).getValue(), //
                pipelinePropertyDef.getPipelinePropertyValues().get(0).getValue());
    }
}
