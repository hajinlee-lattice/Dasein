package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ModelingMetadataUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        URL metadataUrl = ClassLoader.getSystemResource("com/latticeengines/domain/exposed/modeling/metadata.xml");
        
        String contents = FileUtils.readFileToString(new File(metadataUrl.getPath()), Charset.forName("UTF-8"));
        ModelingMetadata metadata = JsonUtils.deserialize(contents, ModelingMetadata.class);
        assertEquals(metadata.getAttributeMetadata().size(), 22);
    }

    @Test(groups = "unit")
    public void testExtensions() throws Exception {
        ModelingMetadata.AttributeMetadata metadata = new ModelingMetadata.AttributeMetadata();
        metadata.setExtensions(Arrays.asList(
                new ModelingMetadata.KV("Category", "Column"),
                new ModelingMetadata.KV("PivotValues", Arrays.asList("V1", "V2"))
        ));
        String serialzied = JsonUtils.serialize(metadata);
        Assert.assertTrue(serialzied.contains("{\"Key\":\"Category\",\"Value\":\"Column\"}"));
        Assert.assertTrue(serialzied.contains("{\"Key\":\"PivotValues\",\"Value\":[\"V1\",\"V2\"]}"));
    }
}
