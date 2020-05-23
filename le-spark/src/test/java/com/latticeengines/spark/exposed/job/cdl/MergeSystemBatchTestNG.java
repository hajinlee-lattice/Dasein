package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeSystemBatchTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testUpsert() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testSingleSystemBatch);
        runnables.add(this::testSystemBatchKeepPrefix);
        runnables.add(this::testHasSystemBatchWithoutPrefix);
        runnables.add(this::testHasSystemBatchPrimarySecondary);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
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

    private void testHasSystemBatchPrimarySecondary() {
        List<String> input = uploadData();
        MergeSystemBatchConfig config = getConfigPrimarySecondary();
        SparkJobResult result = runSparkJob(MergeSystemBatchJob.class, config, input,
                String.format("/tmp/%s/%s/systemBatchPrimarySecondary", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySystemBatchPrimarySecondary));
    }

    private List<String> uploadData() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("template1__Attr1", String.class), //
                Pair.of("template1__" + InterfaceName.CDLCreatedTime, Long.class), //
                Pair.of("template1__" + InterfaceName.CDLUpdatedTime, Long.class), //
                Pair.of("template2__Attr1", String.class), //
                Pair.of("template2__Attr2", String.class), //
                Pair.of("template2__" + InterfaceName.CustomerAccountId.name(), String.class), //
                Pair.of("template2__" + InterfaceName.CDLCreatedTime, Long.class), //
                Pair.of("template2__" + InterfaceName.CDLUpdatedTime, Long.class), //
                Pair.of("template3__Attr1", String.class), //
                Pair.of("template3__Attr2", String.class), //
                Pair.of("template3__" + InterfaceName.CDLCreatedTime, Long.class), //
                Pair.of("template3__" + InterfaceName.CDLUpdatedTime, Long.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "1_1", 1L, 2L, "2_1", "2_2", "2_3", 100L, 200L, "3_1", "3_2", 1000L, 2000L }, //
                { 2, "1_1b", 1000L, 2000L, "2_1b", "2_2b", "2_3b", 100L, 200L, "3_1b", null, 1L, 2L } //
        };
        return data;
    }

    private MergeSystemBatchConfig getConfigForSingleSystem() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setTemplates(Arrays.asList("template2"));
        return config;
    }

    private MergeSystemBatchConfig getConfigForKeepPrefix() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setKeepPrefix(true);
        config.setTemplates(Arrays.asList("template_not_exist1", "template1", "template2", "template_not_exist2"));
        return config;
    }

    private Boolean verifySingleSystemBatch(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 6, record.toString());
            int id = (int) record.get("Id");
            String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
            String attr2 = record.get("Attr2") == null ? null : record.get("Attr2").toString();
            String attr3 = record.get(InterfaceName.CustomerAccountId.name()) == null ? null
                    : record.get(InterfaceName.CustomerAccountId.name()).toString();
            switch (id) {
            case 1:
                Assert.assertEquals(attr1, "2_1", record.toString());
                Assert.assertEquals(attr2, "2_2", record.toString());
                Assert.assertEquals(attr3, "2_3", record.toString());
                break;
            case 2:
                Assert.assertEquals(attr1, "2_1b", record.toString());
                Assert.assertEquals(attr2, "2_2b", record.toString());
                Assert.assertEquals(attr3, "2_3b", record.toString());
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
            Assert.assertEquals(record.getSchema().getFields().size(), 9, record.toString());
            int id = (int) record.get("Id");
            String prefix = "template1__";
            String template1Attr1 = record.get(prefix + "Attr1") == null ? null
                    : record.get(prefix + "Attr1").toString();
            prefix = "template2__";
            String template2Attr1 = record.get(prefix + "Attr1") == null ? null
                    : record.get(prefix + "Attr1").toString();
            String template2Attr2 = record.get(prefix + "Attr2") == null ? null
                    : record.get(prefix + "Attr2").toString();
            String template2attr3 = record.get(prefix + InterfaceName.CustomerAccountId.name()) == null ? null
                    : record.get(prefix + InterfaceName.CustomerAccountId.name()).toString();
            switch (id) {
            case 1:
                Assert.assertEquals(template1Attr1, "1_1", record.toString());
                Assert.assertEquals(template2Attr1, "2_1", record.toString());
                Assert.assertEquals(template2Attr2, "2_2", record.toString());
                Assert.assertEquals(template2attr3, "2_3", record.toString());

                break;
            case 2:
                Assert.assertEquals(template1Attr1, "1_1b", record.toString());
                Assert.assertEquals(template2Attr1, "2_1b", record.toString());
                Assert.assertEquals(template2Attr2, "2_2b", record.toString());
                Assert.assertEquals(template2attr3, "2_3b", record.toString());

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
        config.setTemplates(Arrays.asList("template1_not_exist", "template2", "template3", "template4_not_exist"));
        return config;
    }

    private MergeSystemBatchConfig getConfigPrimarySecondary() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey("Id");
        config.setKeepPrefix(false);
        config.setNotOverwriteByNull(true);
        config.setTemplates(Arrays.asList("template1", "template3", "template2"));
        config.setMinColumns(Arrays.asList(InterfaceName.CDLCreatedTime.name()));
        config.setMaxColumns(Arrays.asList(InterfaceName.CDLUpdatedTime.name()));
        return config;
    }

    private Boolean verifySystemBatchWithoutPrefix(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 6, record.toString());
            int id = (int) record.get("Id");
            String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
            String attr2 = record.get("Attr2") == null ? null : record.get("Attr2").toString();
            String attr3 = record.get(InterfaceName.CustomerAccountId.name()) == null ? null
                    : record.get(InterfaceName.CustomerAccountId.name()).toString();
            switch (id) {
            case 1:
                Assert.assertEquals(attr1, "3_1", record.toString());
                Assert.assertEquals(attr2, "3_2", record.toString());
                Assert.assertEquals(attr3, "2_3", record.toString());
                break;
            case 2:
                Assert.assertEquals(attr1, "3_1b", record.toString());
                Assert.assertEquals(attr2, "2_2b", record.toString());
                Assert.assertEquals(attr3, "2_3b", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifySystemBatchPrimarySecondary(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 6, record.toString());
            int id = (int) record.get("Id");
            String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
            String attr2 = record.get("Attr2") == null ? null : record.get("Attr2").toString();
            String attr3 = record.get(InterfaceName.CustomerAccountId.name()) == null ? null
                    : record.get(InterfaceName.CustomerAccountId.name()).toString();
            Long createTime = Long.valueOf(record.get(InterfaceName.CDLCreatedTime.name()) == null ? null
                    : record.get(InterfaceName.CDLCreatedTime.name()).toString());
            Long updateTime = Long.valueOf(record.get(InterfaceName.CDLCreatedTime.name()) == null ? null
                    : record.get(InterfaceName.CDLUpdatedTime.name()).toString());
            switch (id) {
            case 1:
                Assert.assertEquals(attr1, "2_1", record.toString());
                Assert.assertEquals(attr2, "2_2", record.toString());
                Assert.assertEquals(attr3, "2_3", record.toString());
                Assert.assertEquals((long) createTime, 1L, record.toString());
                Assert.assertEquals((long) updateTime, 2000L, record.toString());
                break;
            case 2:
                Assert.assertEquals(attr1, "2_1b", record.toString());
                Assert.assertEquals(attr2, "2_2b", record.toString());
                Assert.assertEquals(attr3, "2_3b", record.toString());
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
