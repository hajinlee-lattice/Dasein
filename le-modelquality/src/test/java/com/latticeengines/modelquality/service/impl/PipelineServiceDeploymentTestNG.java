package com.latticeengines.modelquality.service.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.modelquality.functionalframework.ModelQualityTestNGBase;
import com.latticeengines.modelquality.service.PipelineService;

public class PipelineServiceDeploymentTestNG extends ModelQualityTestNGBase {

    @Inject
    private PipelineService pipelineService;

    @Test(groups = "deployment", enabled = true)
    public void testValidPipelineJsonPath() throws Exception {

        String pipelineJson = getPrivateFieldValue(pipelineService, "pipelineJson");
        String stackName = getPrivateFieldValue(pipelineService, "stackName");
        Method method = pipelineService.getClass().getSuperclass().getDeclaredMethod("getActiveStack");

        @SuppressWarnings("unchecked")
        String pipelineJsonPath = String.format(pipelineJson, stackName, //
                ((Map<String, String>) method.invoke(pipelineService)).get("ArtifactVersion"));

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, pipelineJsonPath),
                "pipeline.json not found in path : " + pipelineJsonPath);
    }

    private String getPrivateFieldValue(Object obj, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Field f = obj.getClass().getDeclaredField(fieldName); // NoSuchFieldException
        f.setAccessible(true);
        return (String) f.get(obj); // IllegalAccessException
    }

}
