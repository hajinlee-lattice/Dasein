package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryParserTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Test(groups = "functional")
    public void parse() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary.json");
        String data = new String(IOUtils.toByteArray(is));
        ModelSummary summary = modelSummaryParser.parse("/tmp/abc/modelsummary.json", data);
        
        assertEquals(summary.getName(), "Model_Submission1");
        assertEquals(summary.getLookupId(), "TENANT1|Q_EventTable_TENANT1|58e6de15-5448-4009-a512-bd27d59ca75d");
        assertEquals(summary.getTrainingRowCount().longValue(), 17040L);
        assertEquals(summary.getTestRowCount().longValue(), 4294L);
        assertEquals(summary.getTotalRowCount().longValue(), 21334L);
        assertEquals(summary.getTrainingConversionCount().longValue(), 397L);
        assertEquals(summary.getTestConversionCount().longValue(), 105L);
        assertEquals(summary.getTotalConversionCount().longValue(), 502L);
        assertEquals(summary.getRocScore(), 0.8199950607305632);
        
        String decompressedDetails = new String(CompressionUtils.decompressByteArray(summary.getDetails().getData()));
        assertEquals(decompressedDetails, data);
    }

}
