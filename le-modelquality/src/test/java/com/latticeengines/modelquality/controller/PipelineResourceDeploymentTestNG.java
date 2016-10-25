package com.latticeengines.modelquality.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.modelquality.service.impl.PipelineStepType;

public class PipelineResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
        super.cleanupHdfs();
    }

    @Test(groups = "deployment")
    public void createPipelineFromProduction() {
        Pipeline pipeline = modelQualityProxy.createPipelineFromProduction();
        Assert.assertNotNull(pipeline);
    }

    @Test(groups = "deployment", dependsOnMethods = "createPipelineFromProduction")
    public void getPipelines() {
        List<Pipeline> pipelines = modelQualityProxy.getPipelines();
        Assert.assertEquals(pipelines.size(), 1);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", dependsOnMethods = "getPipelines")
    public void getPipelineByName() {
        List<Pipeline> pipelines = modelQualityProxy.getPipelines();
        Pipeline p = modelQualityProxy.getPipelineByName((String) ((Map) pipelines.get(0)).get("name"));
        Assert.assertNotNull(p);
    }

    @Test(groups = "deployment")
    public void uploadPipelineStepPythonFile() throws Exception {
        String step = super.uploadPipelineStepFile(PipelineStepType.PYTHONLEARNING);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/assigncategorical.py");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));
    }

    @Test(groups = "deployment", dependsOnMethods = "uploadPipelineStepPythonFile")
    public void uploadPipelineStepPythonRTSFile() throws Exception {
        String step = super.uploadPipelineStepFile(PipelineStepType.PYTHONRTS);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/assignconversionrate.py");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));
    }

    @Test(groups = "deployment", dependsOnMethods = "uploadPipelineStepPythonRTSFile")
    public void uploadPipelineStepMetadataFile() throws Exception {
        String step = super.uploadPipelineStepFile(PipelineStepType.METADATA);
        assertEquals(step, hdfsDir + "/steps/assigncategorical/metadata.json");
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));
    }    

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", dependsOnMethods = "uploadPipelineStepMetadataFile")
    public void createPipelineFromSteps() throws Exception {
        List<Pipeline> pipelines = modelQualityProxy.getPipelines();
        String str = JsonUtils.serialize((Map) pipelines.get(0));
        Pipeline pipeline = JsonUtils.deserialize(str, Pipeline.class);
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
        
        String pipelineName = modelQualityProxy.createPipeline("P1", pipelineSteps);
        Assert.assertEquals(pipelineName, "P1");
    }

}
