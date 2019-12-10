package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeSystemBatchTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testUpsert() {

        ExecutorService workers = ThreadPoolUtils.getFixedSizeThreadPool("Merge-system-batch-test", 2);
        List<Runnable> runnables = new ArrayList<>();

        Runnable runnable1 = () -> testSingleSystemBatch();
        runnables.add(runnable1);

        Runnable runnable2 = () -> testSystemBatchKeepPrefix();
        runnables.add(runnable2);

        Runnable runnable3 = () -> testHasSystemBatchWithoutPrefix();
        runnables.add(runnable3);

        ThreadPoolUtils.runRunnablesInParallel(workers, runnables, 60, 1);
        workers.shutdownNow();
    }

    private void testSingleSystemBatch() {
        List<String> input = uploadData();
        MergeSystemBatchConfig config = getConfigForSingleSystem();
        SparkJobResult result = runSparkJob(MergeSystemBatchJob.class, config, input,
                String.format("/tmp/%s/%s/singleSystemBatch", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySingleSystemBatch));
    }

    private void testSystemBatchKeepPrefix() {
        List<String> input = uploadData();
        MergeSystemBatchConfig config = getConfigForKeepPrefix();
        SparkJobResult result = runSparkJob(MergeSystemBatchJob.class, config, input,
                String.format("/tmp/%s/%s/systemBatchForKeepPrefix", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySystemBatchForKeepPrefix));
    }

    private void testHasSystemBatchWithoutPrefix() {
        List<String> input = uploadData();
        MergeSystemBatchConfig config = getConfigWithoutPrefix();
        SparkJobResult result = runSparkJob(MergeSystemBatchJob.class, config, input,
                String.format("/tmp/%s/%s/systemBatchWithoutPrefix", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySystemBatchWithoutPrefix));
    }

    private List<String> uploadData() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("system1__Attr1", String.class), //
                Pair.of("system2__Attr1", String.class), //
                Pair.of("system2__Attr2", String.class), //
                Pair.of("system3__Attr1", String.class), //
                Pair.of("system3__Attr2", String.class), //
                Pair.of("system3__Attr3", String.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "1_1", "2_1", "2_2", "3_1", "3_2", "3_3" }, //
                { 2, "1_1b", "2_1b", "2_2b", "3_1b", null, "3_3b" } //
        };
        return data;
    }

    private MergeSystemBatchConfig getConfigForSingleSystem() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setSystems(Arrays.asList("system2"));
        return config;
    }

    private MergeSystemBatchConfig getConfigForKeepPrefix() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setKeepPrefix(true);
        config.setSystems(Arrays.asList("system1", "system2"));
        return config;
    }

    private Boolean verifySingleSystemBatch(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 3, record.toString());
            int id = (int) record.get("Id");
            String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
            String attr2 = record.get("Attr2") == null ? null : record.get("Attr2").toString();
            switch (id) {
            case 1:
                Assert.assertEquals(attr1, "2_1", record.toString());
                Assert.assertEquals(attr2, "2_2", record.toString());
                break;
            case 2:
                Assert.assertEquals(attr1, "2_1b", record.toString());
                Assert.assertEquals(attr2, "2_2b", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifySystemBatchForKeepPrefix(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 4, record.toString());
            int id = (int) record.get("Id");
            String prefix = "system1__";
            String system1Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            prefix = "system2__";
            String system2Attr1 = record.get(prefix + "Attr1") == null ? null : record.get(prefix + "Attr1").toString();
            String system2Attr2 = record.get(prefix + "Attr2") == null ? null : record.get(prefix + "Attr2").toString();
            switch (id) {
            case 1:
                Assert.assertEquals(system1Attr1, "1_1", record.toString());
                Assert.assertEquals(system2Attr1, "2_1", record.toString());
                Assert.assertEquals(system2Attr2, "2_2", record.toString());

                break;
            case 2:
                Assert.assertEquals(system1Attr1, "1_1b", record.toString());
                Assert.assertEquals(system2Attr1, "2_1b", record.toString());
                Assert.assertEquals(system2Attr2, "2_2b", record.toString());

                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private MergeSystemBatchConfig getConfigWithoutPrefix() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setKeepPrefix(false);
        config.setNotOverwriteByNull(true);
        config.setSystems(Arrays.asList("system2", "system3"));
        return config;
    }

    private Boolean verifySystemBatchWithoutPrefix(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 4, record.toString());
            int id = (int) record.get("Id");
            String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
            String attr2 = record.get("Attr2") == null ? null : record.get("Attr2").toString();
            String attr3 = record.get("Attr3") == null ? null : record.get("Attr3").toString();
            switch (id) {
            case 1:
                Assert.assertEquals(attr1, "3_1", record.toString());
                Assert.assertEquals(attr2, "3_2", record.toString());
                Assert.assertEquals(attr3, "3_3", record.toString());
                break;
            case 2:
                Assert.assertEquals(attr1, "3_1b", record.toString());
                Assert.assertEquals(attr2, "2_2b", record.toString());
                Assert.assertEquals(attr3, "3_3b", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

}
