package com.latticeengines.domain.exposed.modelquality;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class PipelineJsonUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/domain/exposed/modelquality/pipeline.json");
        String contents = FileUtils.readFileToString(new File(jsonUrl.getFile()));
        
        PipelineJson json = JsonUtils.deserialize(contents, PipelineJson.class);
        
        assertEquals(json.getSteps().size(), 7);
    }
}
