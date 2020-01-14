package com.latticeengines.spark.exposed.job.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class UpsertWithSystemTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testUpsert() {

        ExecutorService workers = ThreadPoolUtils.getFixedSizeThreadPool("Upsert-with-systems-test", 2);
        List<Runnable> runnables = new ArrayList<>();
        Runnable runnable1 = () -> testHasNoSystemBatch();
        runnables.add(runnable1);
        Runnable runnable2 = () -> testHasSystemBatch();
        runnables.add(runnable2);
        Runnable runnable3 = () -> testHasInputOnly();
        runnables.add(runnable3);

        ThreadPoolUtils.runRunnablesInParallel(workers, runnables, 60, 1);
        workers.shutdownNow();
    }

    private void testHasNoSystemBatch() {
        List<String> input = uploadDataHasNoSystemBatch();
        UpsertConfig config = getHasNoSystemBatchConfig();
        SparkJobResult result = runSparkJob(UpsertJob.class, config, input,
                String.format("/tmp/%s/%s/HasNoSystemBatch", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyHasNoSystemBatch));
    }

    private void testHasSystemBatch() {
        List<String> input = uploadDataHasSystemBatch();
        UpsertConfig config = getHasSystemBatchConfig();

        SparkJobResult result = runSparkJob(UpsertJob.class, config, input,
                String.format("/tmp/%s/%s/HasSystemBatch", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyHasSystemBatch));
    }

    private void testHasInputOnly() {
        List<String> input = uploadHasInputOnly();
        UpsertConfig config = getHasInputOnlyConfig();

        SparkJobResult result = runSparkJob(UpsertJob.class, config, input,
                String.format("/tmp/%s/%s/HasInputOnly", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyHasInputOnly));
    }

    private UpsertConfig getHasInputOnlyConfig() {
        UpsertConfig config = UpsertConfig.joinBy("Id");
        config.setInputSystemBatch(true);
        config.setBatchSystemName(null);
        return config;
    }

    private List<String> uploadHasInputOnly() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = getInput2Fields();
        Object[][] data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private List<String> uploadDataHasNoSystemBatch() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        fields = getInput2Fields();
        data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "1", 1L }, //
                { 2, "2", 2L }, //
                { 3, "3", 3L }, //
                { 4, "4", 4L }, //
        };
        return data;
    }

    private Object[][] getInput2Data() {
        Object[][] data;
        data = new Object[][] { //
                { 2, "22", null, true, "system1" }, //
                { 3, "23", -3L, false, "system1" }, //
                { 2, "24", null, null, "system2" }, //
                { 5, "25", -5L, false, "system2" } //
        };
        return data;
    }

    private List<Pair<String, Class<?>>> getInput2Fields() {
        List<Pair<String, Class<?>>> fields;
        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Boolean.class), //
                Pair.of("__system__", String.class) //
        );
        return fields;
    }

    private UpsertConfig getHasNoSystemBatchConfig() {
        UpsertConfig config = UpsertConfig.joinBy("Id");
        config.setInputSystemBatch(true);
        config.setBatchSystemName("default");
        return config;
    }

    private Boolean verifyHasNoSystemBatch(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            String prefix = "default__";
            String defaultAttr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long defaultAttr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean defaultAttr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            prefix = "system1__";
            String system1Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system1Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system1Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            prefix = "system2__";
            String system2Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system2Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system2Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            switch (id) {
            case 1:
                Assert.assertEquals(defaultAttr1, "1", record.toString());
                Assert.assertEquals(defaultAttr2, Long.valueOf(1), record.toString());
                Assert.assertNull(defaultAttr3, record.toString());

                assertNulls(record, system1Attr1, system1Attr2, system1Attr3);

                assertNulls(record, system2Attr1, system2Attr2, system2Attr3);

                break;
            case 2:
                Assert.assertEquals(defaultAttr1, "2", record.toString());
                Assert.assertEquals(defaultAttr2, Long.valueOf(2), record.toString());
                Assert.assertNull(defaultAttr3, record.toString());

                Assert.assertEquals(system1Attr1, "22", record.toString());
                Assert.assertNull(system1Attr2, record.toString());
                Assert.assertEquals(system1Attr3, Boolean.TRUE, record.toString());

                Assert.assertEquals(system2Attr1, "24", record.toString());
                Assert.assertNull(system2Attr2, record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 3:
                Assert.assertEquals(defaultAttr1, "3", record.toString());
                Assert.assertEquals(defaultAttr2, Long.valueOf(3), record.toString());
                Assert.assertNull(defaultAttr3, record.toString());

                Assert.assertEquals(system1Attr1, "23", record.toString());
                Assert.assertEquals(system1Attr2, Long.valueOf(-3), record.toString());
                Assert.assertEquals(system1Attr3, Boolean.FALSE, record.toString());

                assertNulls(record, system2Attr1, system2Attr2, system2Attr3);

                break;
            case 4:
                Assert.assertEquals(defaultAttr1, "4", record.toString());
                Assert.assertEquals(defaultAttr2, Long.valueOf(4), record.toString());
                Assert.assertNull(defaultAttr3, record.toString());

                assertNulls(record, system1Attr1, system1Attr2, system1Attr3);
                assertNulls(record, system2Attr1, system2Attr2, system2Attr3);

                break;
            case 5:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);
                assertNulls(record, system1Attr1, system1Attr2, system1Attr3);

                Assert.assertEquals(system2Attr1, "25", record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(-5), record.toString());
                Assert.assertEquals(system2Attr3, Boolean.FALSE, record.toString());

                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
        });

        return true;
    }

    private void assertNulls(GenericRecord record, String attr1, Long attr2, Boolean attr3) {
        Assert.assertNull(attr1, record.toString());
        Assert.assertNull(attr2, record.toString());
        Assert.assertNull(attr3, record.toString());
    }

    private UpsertConfig getHasSystemBatchConfig() {
        UpsertConfig config = UpsertConfig.joinBy("Id");
        config.setInputSystemBatch(true);
        config.setBatchSystemName(null);
        return config;
    }

    private List<String> uploadDataHasSystemBatch() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("system1__Attr1", String.class), //
                Pair.of("system2__Attr2", Long.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        fields = getInput2Fields();
        data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private Boolean verifyHasSystemBatch(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            String prefix = "default__";
            String defaultAttr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long defaultAttr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean defaultAttr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            prefix = "system1__";
            String system1Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system1Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system1Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            prefix = "system2__";
            String system2Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system2Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system2Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            switch (id) {
            case 1:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);

                Assert.assertEquals(system1Attr1, "1", record.toString());
                Assert.assertNull(system1Attr2, record.toString());
                Assert.assertNull(system1Attr3, record.toString());

                Assert.assertNull(system2Attr1, record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(1), record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 2:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);

                Assert.assertEquals(system1Attr1, "22", record.toString());
                Assert.assertNull(system1Attr2, record.toString());
                Assert.assertEquals(system1Attr3, Boolean.TRUE, record.toString());

                Assert.assertEquals(system2Attr1, "24", record.toString());
                Assert.assertNull(system2Attr2, record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 3:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);

                Assert.assertEquals(system1Attr1, "23", record.toString());
                Assert.assertEquals(system1Attr2, Long.valueOf(-3), record.toString());
                Assert.assertEquals(system1Attr3, Boolean.FALSE, record.toString());

                Assert.assertNull(system2Attr1, record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(3), record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 4:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);

                Assert.assertEquals(system1Attr1, "4", record.toString());
                Assert.assertNull(system1Attr2, record.toString());
                Assert.assertNull(system1Attr3, record.toString());

                Assert.assertNull(system2Attr1, record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(4), record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 5:
                assertNulls(record, defaultAttr1, defaultAttr2, defaultAttr3);
                assertNulls(record, system1Attr1, system1Attr2, system1Attr3);

                Assert.assertEquals(system2Attr1, "25", record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(-5), record.toString());
                Assert.assertEquals(system2Attr3, Boolean.FALSE, record.toString());

                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
        });

        return true;
    }

    private Boolean verifyHasInputOnly(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            String prefix = "system1__";
            String system1Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system1Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system1Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            prefix = "system2__";
            String system2Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            Long system2Attr2 = record.get(prefix + "Attr2") == null ? null : (long) record.get(prefix + "Attr2");
            Boolean system2Attr3 = record.get(prefix + "Attr3") == null ? null : (boolean) record.get(prefix + "Attr3");
            switch (id) {
            case 2:
                Assert.assertEquals(system1Attr1, "22", record.toString());
                Assert.assertNull(system1Attr2, record.toString());
                Assert.assertEquals(system1Attr3, Boolean.TRUE, record.toString());

                Assert.assertEquals(system2Attr1, "24", record.toString());
                Assert.assertNull(system2Attr2, record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 3:
                Assert.assertEquals(system1Attr1, "23", record.toString());
                Assert.assertEquals(system1Attr2, Long.valueOf(-3), record.toString());
                Assert.assertEquals(system1Attr3, Boolean.FALSE, record.toString());

                Assert.assertNull(system2Attr1, record.toString());
                Assert.assertNull(system2Attr2, record.toString());
                Assert.assertNull(system2Attr3, record.toString());

                break;
            case 5:
                assertNulls(record, system1Attr1, system1Attr2, system1Attr3);

                Assert.assertEquals(system2Attr1, "25", record.toString());
                Assert.assertEquals(system2Attr2, Long.valueOf(-5), record.toString());
                Assert.assertEquals(system2Attr3, Boolean.FALSE, record.toString());

                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
        });

        return true;
    }

}
