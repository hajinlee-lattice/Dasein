package com.latticeengines.datacloud.dataflow.transformation.source;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;


public class ConsolidateCollectionAlexaFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ConsolidateCollectionAlexaFlow.class);
    private static final String FIELD_DOMAIN = "URL";
    private static final String FIELD_TIMESTAMP = "LE_Last_Upload_Date";
    private static final String SAMPLE_AVRO_PATH = "transformation.source/alexa-sample.avro";

    @Override
    protected String getFlowBeanName() {
        return ConsolidateCollectionAlexaFlow.BEAN_NAME;
    }


    @Test(groups = "functional")
    public void testRunFlow() {
        try {
            TransformationFlowParameters parameters = prepareInput();
            executeDataFlow(parameters);
            verifyResult();
        }catch (Exception e) {
            log.info(e.getMessage(), e);
            Assert.assertTrue(false);
        }
    }

    private TransformationFlowParameters prepareInput() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader()
                .getResource(SAMPLE_AVRO_PATH);
        File tmpFile = File.createTempFile("alexa", "avro");
        FileUtils.copyURLToFile(url, tmpFile);

        copyAvro(tmpFile.getPath(), AVRO_INPUT, AVRO_DIR);

        ConsolidateCollectionParameters parameters = new ConsolidateCollectionParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setGroupBy(Collections.singletonList(FIELD_DOMAIN));
        parameters.setSortBy(FIELD_TIMESTAMP);
        return parameters;
    }

    private Map<String, GenericRecord> aggMostRecent() throws Exception {

        Map<String, GenericRecord> ret = new HashMap<>();

        List<GenericRecord> recList = AvroUtils.readFromInputStream(
                Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(SAMPLE_AVRO_PATH)
        );

        for (GenericRecord rec: recList) {

            String domain = rec.get(FIELD_DOMAIN).toString();

            if (!ret.containsKey(domain)) {

                ret.put(domain, rec);

            } else {

                GenericRecord prevRec = ret.get(domain);
                if ((long)rec.get(FIELD_TIMESTAMP) > (long)prevRec.get(FIELD_TIMESTAMP)) {

                    ret.put(domain, rec);
                }
            }
        }
        recList.clear();

        return ret;

    }

    private void verifyResult() throws Exception {

        log.info("reading output...");
        List<GenericRecord> records = readOutput();
        Assert.assertNotEquals(records.size(), 0);

        log.info("calculating expected output...");
        Map<String, GenericRecord> expectedResults = aggMostRecent();

        log.info("comparing output and expected output...");
        for (GenericRecord rec: records) {
            //just check timestamp
            String domain = rec.get(FIELD_DOMAIN).toString();
            Assert.assertTrue(expectedResults.containsKey(domain));

            long expectedTs = (long) expectedResults.get(domain).get(FIELD_TIMESTAMP);
            long executionTs = (long) rec.get(FIELD_TIMESTAMP);
            Assert.assertEquals(expectedTs, executionTs);
        }

        log.info("complete without problem found");

    }

}
