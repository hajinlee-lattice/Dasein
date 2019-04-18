package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ConsolidateCollectionBWFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final String FIELD_DOMAIN = "Domain";
    private static final String FIELD_TECH_NAME = "Technology_Name";
    private static final String FIELD_TIMESTAMP = "Last_Modification_Date";

    @Override
    protected String getFlowBeanName() {
        return ConsolidateCollectionBWFlow.BEAN_NAME;
    }


    @Test(groups = "functional")
    public void testRunFlow() {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(FIELD_DOMAIN, String.class), //
                Pair.of(FIELD_TECH_NAME, String.class),
                Pair.of(FIELD_TIMESTAMP, Long.class)
        );
        Object[][] data = new Object[][] { //
                { "url_1", "1", 1L }, //
                { "url_1", "1", 2L }, //
                { "url_1", "2", 3L }, //
                { "url_1", "2", 2L }, //
                { "url_1", "3", 4L }, //
                { "url_2", "1", 2L }, //
                { "url_2", "1", 5L }, //
        };
        uploadDataToSharedAvroInput(data, fields);

        ConsolidateCollectionParameters parameters = new ConsolidateCollectionParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setGroupBy(Arrays.asList(FIELD_DOMAIN, FIELD_TECH_NAME));
        parameters.setSortBy(FIELD_TIMESTAMP);
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 4);
        for (GenericRecord record : records) {
            long ts = (long) record.get(FIELD_TIMESTAMP);
            String combKey = record.get(FIELD_TECH_NAME).toString() + "-" + record.get(FIELD_DOMAIN).toString();
            long expectedTs = 0;
            switch (combKey) {
                case "1-url_1":
                    expectedTs = 2L;
                    break;
                case "1-url_2":
                    expectedTs = 5L;
                    break;
                case "2-url_1":
                    expectedTs = 3L;
                    break;
                case "3-url_1":
                    expectedTs = 4L;
                    break;
                default:
                    Assert.fail("Unexpected record: " + record.toString());
            }
            //System.out.println(record);
            Assert.assertEquals(ts, expectedTs);
        }
    }




}
