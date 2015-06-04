package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ModelingMetadataUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        URL metadataUrl = ClassLoader.getSystemResource("com/latticeengines/domain/exposed/modeling/metadata.xml");
        
        String contents = FileUtils.readFileToString(new File(metadataUrl.getPath()));
        ModelingMetadata metadata = JsonUtils.deserialize(contents, ModelingMetadata.class);
        assertEquals(metadata.getAttributeMetadata().size(), 22);
    }
}
