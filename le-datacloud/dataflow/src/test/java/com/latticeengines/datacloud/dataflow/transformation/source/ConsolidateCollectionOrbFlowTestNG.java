package com.latticeengines.datacloud.dataflow.transformation.source;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ConsolidateCollectionOrbFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private final static Logger log = LoggerFactory.getLogger(ConsolidateCollectionOrbFlowTestNG.class);
    private final static String FIELD_DOMAIN = "Domain";
    private final static String FIELD_TIMESTAMP = "LE_Last_Upload_Date";

    @Override
    protected String getFlowBeanName() {
        return ConsolidateCollectionOrbFlow.BEAN_NAME;
    }


    @Test(groups = "functional1")
    public void testRunFlow() {
        try {
            TransformationFlowParameters parameters = prepareInput();
            executeDataFlow(parameters);
            verifyResult();
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private TransformationFlowParameters prepareInput() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader()
                .getResource("transformation.source/orb-sample.avro");
        File tmpFile = File.createTempFile("alexa", "avro");
        FileUtils.copyURLToFile(url, tmpFile);

        copyAvro(tmpFile.getPath(), AVRO_INPUT, AVRO_DIR);

        ConsolidateCollectionParameters parameters = new ConsolidateCollectionParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setGroupBy(Arrays.asList(FIELD_DOMAIN));
        parameters.setSortBy(FIELD_TIMESTAMP);
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertNotEquals(records.size(), 0);
        for (GenericRecord record : records) {
            log.info(record.toString());
        }
    }


}