package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.PipelineService;
import com.latticeengines.proxy.exposed.pls.PlsHealthCheckProxy;

public class PipelineServiceImplTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private PipelineService pipelineService;

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    private PlsHealthCheckProxy plsHealthCheckProxy = null;

    protected Pipeline pipeline;
    protected Pipeline newPipeline;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        plsHealthCheckProxy = mock(PlsHealthCheckProxy.class);
        Map<String, String> activeStack = new HashMap<>();
        activeStack.put("CurrentStack", "");
        when(plsHealthCheckProxy.getActiveStack()).thenReturn(activeStack);
        String pipelineStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/pipeline.json")
                        .getFile()),
                Charset.defaultCharset());
        pipeline = JsonUtils.deserialize(pipelineStr, Pipeline.class);
        pipelineEntityMgr.create(pipeline);
    }

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        pipelineEntityMgr.delete(pipeline);
        pipelineEntityMgr.delete(newPipeline);
        cleanupHdfs();
    }

    @Test(groups = "functional", enabled = false)
    public void uploadPipelineStepFileForMetadata() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues.json");
        String step = pipelineService.uploadPipelineStepFile("assigncategorical", is, new String[] { "", "json" },
                PipelineStepType.METADATA);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/metadata.json");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));
    }

    @Test(groups = "functional", dependsOnMethods = { "uploadPipelineStepFileForMetadata" }, enabled = false)
    public void uploadPipelineStepFileForPython() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues.py");
        String step = pipelineService.uploadPipelineStepFile("assigncategorical", is, new String[] { "", "py" },
                PipelineStepType.PYTHONLEARNING);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/assigncategorical.py");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));

    }

    @Test(groups = "functional", dependsOnMethods = { "uploadPipelineStepFileForPython" }, enabled = false)
    public void createPipeline() {
        List<PipelineStepOrFile> pipelineSteps = new ArrayList<>();

        for (PipelineStep step : pipeline.getPipelineSteps()) {
            PipelineStepOrFile p = new PipelineStepOrFile();

            if (!step.getMainClassName().equals("EnumeratedColumnTransformStep")) {
                p.pipelineStepName = step.getName();
            } else {
                Path path = new Path(hdfsDir + "/steps/assigncategorical");
                p.pipelineStepDir = path.toString();
            }

            pipelineSteps.add(p);
        }

        newPipeline = pipelineService.createPipeline("ModelQualityFunctionalTest", "ModelQualityFunctionalTest",
                pipelineSteps);
        assertTrue(newPipeline.getPipelineDriver().contains("pipelines/ModelQualityFunctionalTest/pipeline-"));
        assertEquals(newPipeline.getPipelineSteps().size(), 7);
    }

    @Test(groups = "functional", enabled = false)
    public void createPipelineFailure() {
        List<PipelineStepOrFile> pipelineSteps = new ArrayList<>();
        PipelineStepOrFile p = new PipelineStepOrFile();
        p.pipelineStepName = "Invalid_step_name";
        pipelineSteps.add(p);

        try {
            pipelineService.createPipeline("P1-Fail", "Trying to create invalid pipeline", pipelineSteps);
        } catch (LedpException e) {
            // expected, so do nothing
        }
        Pipeline result = pipelineEntityMgr.findByName("P1-Fail");
        assertNull(result);
    }

}
