package com.latticeengines.modelquality.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

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
    public void uploadPipelineStepFile() throws Exception {
        try {
            LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
            Resource resource = new ClassPathResource("com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues.py");
            map.add("file", resource);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
            String fileName = "assignconversionratetoallcategoricalvalues.py";
            
            String step = modelQualityProxy.uploadPipelineStepPythonScript(fileName, "assigncategorical", requestEntity);
            assertEquals(step, hdfsDir + "/steps/assigncategorical/assigncategorical.py");
            assertTrue(HdfsUtils.fileExists(yarnConfiguration, step));

            
        } catch (Exception ex) {
            Assert.fail("Failed!", ex);
        }
    }
}
