package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertTrue;

import java.io.InputStream;

import com.latticeengines.domain.exposed.workflow.KeyValue;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;

public class KeyValueUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/domain/exposed/pls/modelsummary.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        
        details.setData(data);
        String serializedStr = details.toString();
        
        KeyValue deserializedKeyValue = JsonUtils.deserialize(serializedStr, KeyValue.class);
        
        String originalStrData = new String(CompressionUtils.decompressByteArray(deserializedKeyValue.getData()));
        assertTrue(originalStrData.contains("\"Segmentations\":"));
    }
}
