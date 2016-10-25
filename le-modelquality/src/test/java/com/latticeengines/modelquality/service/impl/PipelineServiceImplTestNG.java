package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.PipelineService;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class PipelineServiceImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    @Autowired
    private PipelineService pipelineService;
    
    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;
    
    private InternalResourceRestApiProxy proxy = null;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.cleanupDb();
        super.cleanupHdfs();

        proxy = mock(InternalResourceRestApiProxy.class);
        Map<String, String> activeStack = new HashMap<>();
        activeStack.put("CurrentStack", "");
        when(proxy.getActiveStack()).thenReturn(activeStack);
        ReflectionTestUtils.setField(pipelineService, "internalResourceRestApiProxy", proxy);
    }

    @Test(groups = "functional")
    public void createLatestProductionPipeline() {
        pipelineService.createLatestProductionPipeline();
    }
    
    @Test(groups = "functional", dependsOnMethods = { "createLatestProductionPipeline" })
    public void uploadPipelineStepFileForMetadata() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues.json");
        String step = pipelineService.uploadPipelineStepFile("assigncategorical", is, new String[] { "", "json"}, PipelineStepType.METADATA);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/metadata.json");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));
    }
    
    @Test(groups = "functional", dependsOnMethods = { "uploadPipelineStepFileForMetadata" })
    public void uploadPipelineStepFileForPython() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues.py");
        String step = pipelineService.uploadPipelineStepFile("assigncategorical", is, new String[] { "", "py" }, PipelineStepType.PYTHONLEARNING);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/assigncategorical.py");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));

    }
    
    @Test(groups = "functional", dependsOnMethods = { "uploadPipelineStepFileForPython" })
    public void createPipeline() {
        Pipeline pipeline = pipelineEntityMgr.findAll().get(0);
        
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
        
        Pipeline newPipeline = pipelineService.createPipeline("P1", pipelineSteps);
        assertTrue(newPipeline.getPipelineDriver().contains("pipelines/P1/pipeline-"));
        assertEquals(newPipeline.getPipelineSteps().size(), 6);
    }
    
}
