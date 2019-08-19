package com.latticeengines.spark.testframework;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.LID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

public class TestDedupEventTableTestNGBase extends SparkJobFunctionalTestNGBase {
    protected List<String> inputs;

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is my output!");
    }

    @Override
    protected List<String> getInputOrder() {
        return inputs;
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyOutput);
    }

    private Boolean verifyOutput(HdfsDataUnit target) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            Integer id = Integer.parseInt(record.get("ID").toString());
            Object event = record.get("Event");
            Boolean eventBoolVal = null;
            if (event != null) {
                eventBoolVal = Boolean.valueOf(event.toString());
            }
            Object latAccId = record.get(LID_FIELD);
            Long latAccIdLongVal = null;
            if (latAccId != null) {
                latAccIdLongVal = Long.valueOf(latAccId.toString());
            }
            Object intLdcLid = record.get(INT_LDC_LID);
            String intLdcLidStrVal = null;
            if (intLdcLid != null) {
                intLdcLidStrVal = intLdcLid.toString();
            }
            switch (id) {
                case 2:
                    Assert.assertEquals(eventBoolVal, Boolean.TRUE);
                    Assert.assertEquals(latAccIdLongVal, Long.valueOf(2L));
                    Assert.assertEquals(intLdcLidStrVal, "2");
                    break;
                case 4:
                    Assert.assertEquals(eventBoolVal, Boolean.TRUE);
                    Assert.assertEquals(latAccIdLongVal, Long.valueOf(4L));
                    Assert.assertEquals(intLdcLidStrVal, "4");
                    break;
                case 7:
                    Assert.assertEquals(eventBoolVal, Boolean.TRUE);
                    Assert.assertEquals(latAccIdLongVal, Long.valueOf(7L));
                    Assert.assertEquals(intLdcLidStrVal, "7");
                    break;
                case 11:
                    Assert.assertEquals(eventBoolVal, Boolean.TRUE);
                    Assert.assertEquals(latAccIdLongVal, Long.valueOf(10L));
                    Assert.assertEquals(intLdcLidStrVal, "11");
                    break;
                case 12:
                    Assert.assertEquals(eventBoolVal, Boolean.TRUE);
                    Assert.assertEquals(latAccIdLongVal, Long.valueOf(10L));
                    Assert.assertEquals(intLdcLidStrVal, "12");
                    break;
                default:
                    Assert.fail("Unexpected group by key value: " + id);
            }
        });
        Assert.assertEquals(count.get(), 5);
        return true;
    }

    protected void uploadInputAvro() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(Pair.of("ID", Integer.class), //
                Pair.of("Event", Boolean.class), //
                Pair.of(LID_FIELD, Long.class), //
                Pair.of(INT_LDC_DEDUPE_ID, String.class), //
                Pair.of(INT_LDC_LID, String.class), //
                Pair.of(INT_LDC_REMOVED, Integer.class)); //
        Object[][] data = new Object[][] { //
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
        String data1 = uploadHdfsDataUnit(data, fields);
        inputs = Arrays.asList(data1);
    }
}
