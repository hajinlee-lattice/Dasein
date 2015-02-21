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
                "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        String data = new String(IOUtils.toByteArray(is));
        ModelSummary summary = modelSummaryParser.parse("/tmp/abc/modelsummary.json", data);
        
        assertEquals(summary.getName(), "PLSModel-Eloqua");
        assertEquals(summary.getLookupId(), "TENANT1|Q_PLS_Modeling_TENANT1|8195dcf1-0898-4ad3-b94d-0d0f806e979e");
        assertEquals(summary.getTrainingRowCount().longValue(), 15376L);
        assertEquals(summary.getTestRowCount().longValue(), 3738L);
        assertEquals(summary.getTotalRowCount().longValue(), 19114L);
        assertEquals(summary.getTrainingConversionCount().longValue(), 719L);
        assertEquals(summary.getTestConversionCount().longValue(), 154L);
        assertEquals(summary.getTotalConversionCount().longValue(), 873L);
        assertEquals(summary.getRocScore(), 0.9341374179555253);
        
        String decompressedDetails = new String(CompressionUtils.decompressByteArray(summary.getDetails().getData()));
        assertEquals(decompressedDetails, data);
    }

}
