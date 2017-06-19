package com.latticeengines.leadprioritization.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.LID_FIELD;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-dataflow-context.xml" })
public class DedupEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        DedupEventTableParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    private DedupEventTableParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(Pair.of("ID", Integer.class), //
                Pair.of("Event", Boolean.class), //
                Pair.of(LID_FIELD, Long.class), //
                Pair.of(INT_LDC_DEDUPE_ID, String.class), //
                Pair.of(INT_LDC_LID, String.class), //
                Pair.of(INT_LDC_REMOVED, Integer.class)); //

        uploadDataToSharedAvroInput(getData(), fields);

        DedupEventTableParameters parameters = new DedupEventTableParameters();
        parameters.eventTable = AVRO_INPUT;
        return parameters;
    }

    private Object[][] getData() {
        return new Object[][] { //
                { 1, false, 1L, "1", "1", 0 }, //
                { 2, true, 2L, "1", "2", 0 }, //
                { 4, true, 4L, "2", "4", 0 }, //
                { 5, false, 5L, "2", "5", 0 }, //
                { 6, true, 6L, "2", "6", 0 }, //
                { 7, true, 7L, "3", "7", 0 }, //
                { 8, true, 8L, "3", "8", 0 }, //

                { 9, true, 9L, "3", "9", 1 }, //
                { 10, true, 10L, "3", "10", 1 }, //

                { 11, true, 10L, null, "11", 0 }, //
                { 12, true, 10L, null, "12", 0 }, //
        };
    }

    private void verifyResult() {
        int numRows = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            numRows++;
            String id = String.valueOf(record.get("ID"));
            recordMap.put(id, record);
        }
        Assert.assertEquals(numRows, 5);

        GenericRecord record = recordMap.get("2");
        Assert.assertNotNull(record);
        GenericRecord record1 = recordMap.get("4");
        GenericRecord record2 = recordMap.get("6");
        Assert.assertTrue(record1 != null || record2 != null);

        record = recordMap.get("9");
        Assert.assertNull(record);
        record = recordMap.get("10");
        Assert.assertNull(record);

        record = recordMap.get("11");
        Assert.assertNotNull(record);
        record = recordMap.get("12");
        Assert.assertNotNull(record);
    }

}
