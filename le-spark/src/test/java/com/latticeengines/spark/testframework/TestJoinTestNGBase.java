package com.latticeengines.spark.testframework;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

public abstract class TestJoinTestNGBase extends SparkJobFunctionalTestNGBase {

    private List<String> inputs;

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is my output!");
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyOutput1, this::verifyOutput2);
    }

    @Override
    protected List<String> getInputOrder() {
        return inputs;
    }

    private Boolean verifyOutput1(HdfsDataUnit target) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            String key = record.get("Field1").toString();
            Long cnt = (Long) record.get("Cnt");
            switch (key) {
                case "1":
                    Assert.assertEquals(cnt.longValue(), 2);
                    break;
                case "2":
                    Assert.assertEquals(cnt.longValue(), 1);
                    break;
                case "3":
                    Assert.assertEquals(cnt.longValue(), 1);
                    break;
                default:
                    Assert.fail("Unexpected group by key value: " + key);
            }
        });
        Assert.assertEquals(count.get(), 3);
        return true;
    }

    private Boolean verifyOutput2(HdfsDataUnit target) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            String key = record.get("Field1").toString();
            Integer max1 = (Integer) record.get("Max1");
            Integer max2 = (Integer) record.get("Max2");
            switch (key) {
                case "1":
                    Assert.assertEquals(max1.intValue(), 2);
                    Assert.assertEquals(max2.intValue(), 1);
                    break;
                case "2":
                    Assert.assertEquals(max1.intValue(), 3);
                    Assert.assertEquals(max2.intValue(), 2);
                    break;
                case "3":
                    Assert.assertNull(max1);
                    Assert.assertEquals(max2.intValue(), 3);
                    break;
                default:
                    Assert.fail("Unexpected group by key value: " + key);
            }
        });
        Assert.assertEquals(count.get(), 3);
        return true;
    }

    protected void uploadInputAvro() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Long.class), //
                Pair.of("Field1", String.class), //
                Pair.of("Field2", Integer.class) //
        );
        Object[][] data = new Object[][] { //
                { 0L, "1", 1}, //
                { 1L, "1", 2}, //
                { 2L, "2", 3}, //
        };
        String data1 = uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Long.class), //
                Pair.of("Field1", String.class), //
                Pair.of("Field2", Integer.class) //
        );
        data = new Object[][] { //
                { 0L, "1", 1},    //
                { 1L, "2", 2},    //
                { 2L, "3", 3},    //
        };
        String data2 = uploadHdfsDataUnit(data, fields);

        inputs = Arrays.asList(data1, data2);
    }

}
