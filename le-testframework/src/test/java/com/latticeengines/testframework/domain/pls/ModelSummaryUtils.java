package com.latticeengines.testframework.domain.pls;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;

public class ModelSummaryUtils {

    public static ModelSummary generateModelSummary(Tenant tenant, String modelSummaryJsonLocalResourcePath)
            throws IOException {
        ModelSummary summary;

        summary = new ModelSummary();
        summary.setId("123");
        summary.setName("Model1");
        summary.setRocScore(0.75);
        summary.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        summary.setTrainingRowCount(8000L);
        summary.setTestRowCount(2000L);
        summary.setTotalRowCount(10000L);
        summary.setTrainingConversionCount(80L);
        summary.setTestConversionCount(20L);
        summary.setTotalConversionCount(100L);
        summary.setConstructionTime(System.currentTimeMillis());
        summary.setTenant(tenant);

        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(modelSummaryJsonLocalResourcePath);
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);

        return summary;
    }

}
