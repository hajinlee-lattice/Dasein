package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class PipelineResourceTestNG extends ModelQualityDeploymentTestNGBase {

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Test(groups = "deployment")
    public void upsertPipelines() {
        try {
            Pipeline pipeline1 = createPipeline(1);
            Pipeline pipeline2 = createPipeline(2);
            pipeline2.getPipelineSteps().get(0).addPipeline(pipeline1);

            ResponseDocument<String> response = modelQualityProxy.upsertPipelines(Arrays.asList(pipeline1, pipeline2));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertPipelines")
    public void getPipelines() {
        try {
            ResponseDocument<List<Pipeline>> response = modelQualityProxy.getPipelines();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 2);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment")
    public void uploadPipelineStepFile() {
        try {
            LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
            String code = "import os";
            Resource resource = new ClassPathResource("com/latticeengines/modelquality/controller/teststep.py");
            map.add("file", resource);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);

            String fileName = "customtargetstep.py";
            ResponseDocument<String> response = modelQualityProxy.uploadPipelineStepFile(fileName, requestEntity);
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult(), hdfsDir + "/" + fileName);

            String actualCode = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsDir + "/" + fileName);
            Assert.assertEquals(actualCode, code + "");

        } catch (Exception ex) {
            Assert.fail("Failed!", ex);
        }
    }
}
