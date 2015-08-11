package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MetadataUnitTestNG {
    @Test(groups = "unit")
    public void testSerializationAndDeseralization() throws Exception {
        URL metadataUrl = ClassLoader.getSystemResource("com/latticeengines/domain/exposed/modeling/metadata.avsc");
        String metadataContents = FileUtils.readFileToString(new File(metadataUrl.getPath()));

        ObjectMapper objectMapper = new ObjectMapper();
        Metadata metadata = objectMapper.readValue(metadataContents, Metadata.class);
        assertEquals(metadata.getStatus(), 3);
        assertEquals(metadata.getAttributeMetadata().size(), 244);
        assertEquals(metadata.getErrorMessage(), null);

    }
}
