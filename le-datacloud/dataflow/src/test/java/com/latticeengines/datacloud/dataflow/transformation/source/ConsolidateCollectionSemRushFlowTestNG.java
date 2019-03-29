package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ConsolidateCollectionSemRushFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private final static Logger log = LoggerFactory.getLogger(ConsolidateCollectionSemrushFlow.class);
    private final static String FIELD_DOMAIN = "Domain";
    private final static String FIELD_TIMESTAMP = "Last_Modification_Date";
    private final static String FIELD_RANK = "Rank";

    @Override
    protected String getFlowBeanName() {
        return ConsolidateCollectionSemrushFlow.BEAN_NAME;
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
                Pair.of(FIELD_TIMESTAMP, Long.class),
                Pair.of(FIELD_RANK, Integer.class)
        );
        Object[][] data = new Object[][] { //
                {"url_1", 1L, 4}, //
                {"url_1", 2L, 5}, //
                {"url_1", 3L, 6}, //
                {"url_1", 2L, 1}, //
                {"url_1", 4L, 0}, //
                {"url_2", 2L, 3}, //
                {"url_2", 5L, 2}, //
        };
        uploadDataToSharedAvroInput(data, fields);

        ConsolidateCollectionParameters parameters = new ConsolidateCollectionParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setGroupBy(Arrays.asList(FIELD_DOMAIN));
        parameters.setSortBy(FIELD_TIMESTAMP);
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 2);
        for (GenericRecord record : records) {
            long ts = (long) record.get(FIELD_TIMESTAMP);
            String combKey = record.get(FIELD_DOMAIN).toString();
            long expectedTs = 0;
            switch (combKey) {
                case "url_1":
                    expectedTs = 4L;
                    break;
                case "url_2":
                    expectedTs = 5L;
                    break;
                default:
                    Assert.fail("Unexpected record: " + record.toString());
            }
            log.info(record.toString());
            Assert.assertEquals(ts, expectedTs);

            Assert.assertTrue((expectedTs != 4L) || record.get(FIELD_RANK) == null,
                    "when time stamp is 4, Rank will become null");

            Assert.assertTrue((expectedTs != 5L) || (int)record.get(FIELD_RANK) == 2,
                    "when time stamp is 5, Rank will be 2");

        }
    }


}
