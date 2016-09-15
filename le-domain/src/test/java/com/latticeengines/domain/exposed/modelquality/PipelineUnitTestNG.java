package com.latticeengines.domain.exposed.modelquality;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class PipelineUnitTestNG {

    @Test(groups = "unit")
    public void addStepsFromPipelineJson() throws Exception {
        URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/domain/exposed/modelquality/pipeline.json");
        String contents = FileUtils.readFileToString(new File(jsonUrl.getFile()));
        
        Pipeline pipeline = new Pipeline();
        pipeline.addStepsFromPipelineJson(contents);
        
        assertEquals(pipeline.getPipelineSteps().size(), 7);
        
        String serializedPipeline = JsonUtils.serialize(pipeline);
        Pipeline deserializedPipeline = JsonUtils.deserialize(serializedPipeline, Pipeline.class);
        assertEquals(deserializedPipeline.getPipelineSteps().size(), 7);
    }
}
