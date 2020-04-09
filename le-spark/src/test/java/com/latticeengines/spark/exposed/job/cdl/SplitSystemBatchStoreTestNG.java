package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SplitSystemBatchStoreConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitSystemBatchStoreTestNG extends SparkJobFunctionalTestNGBase {
    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testSingleTemplateSystemBatch);
        runnables.add(this::testMultiTemplateSystemBatch);
        runnables.add(this::testSystemBatchWithDiscardFields);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testSingleTemplateSystemBatch() {
        List<String> input = prepareData();
        SplitSystemBatchStoreConfig config = getConfigForTemplateOne();
        SparkJobResult result = runSparkJob(SplitSystemBatchStoreJob.class, config, input,
                String.format("/tmp/%s/%s/singleTemplatesSystemBatch", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyTemplateOneSplit));
    }

    private void testMultiTemplateSystemBatch() {
        List<String> input = prepareData();
        SplitSystemBatchStoreConfig config = getConfigForAllTemplates();
        SparkJobResult result = runSparkJob(SplitSystemBatchStoreJob.class, config, input,
                String.format("/tmp/%s/%s/multiTemplatesSystemBatch", leStack, this.getClass().getSimpleName()));
        Assert.assertEquals(result.getTargets().size(), 3);
        verify(result, Arrays.asList(this::verifyTemplateOneSplit, this::verifyTemplateTwoSplit,
                this::verifyTemplateThreeSplit));
    }

    private void testSystemBatchWithDiscardFields() {
        List<String> input = prepareData();
        SplitSystemBatchStoreConfig config = getConfigForTemplateTwoWithDiscards();
        SparkJobResult result = runSparkJob(SplitSystemBatchStoreJob.class, config, input,
                String.format("/tmp/%s/%s/SystemBatchWithDiscardFields", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyTemplateTwoWithDiscards));
    }

    private SplitSystemBatchStoreConfig getConfigForTemplateOne() {
        SplitSystemBatchStoreConfig config = new SplitSystemBatchStoreConfig();
        List<String> templates = new LinkedList<>();
        templates.add("template1");
        config.setTemplates(templates);
        return config;
    }

    private SplitSystemBatchStoreConfig getConfigForTemplateTwoWithDiscards() {
        SplitSystemBatchStoreConfig config = new SplitSystemBatchStoreConfig();
        List<String> templates = new LinkedList<>();
        templates.add("template2");
        List<String> discardFields = new LinkedList<>();
        discardFields.add("Attr3");
        config.setTemplates(templates);
        config.setDiscardFields(discardFields);
        return config;
    }

    private SplitSystemBatchStoreConfig getConfigForAllTemplates() {
        SplitSystemBatchStoreConfig config = new SplitSystemBatchStoreConfig();
        List<String> templates = new LinkedList<>();
        templates.add("template1");
        templates.add("template2");
        templates.add("template3");
        config.setTemplates(templates);
        return config;
    }

    private Boolean verifyTemplateOneSplit(HdfsDataUnit templateone) {
        // for template 1, output should be:
        //+-----+--------------+--------------+---+-----------------+
        //|Attr1|CDLCreatedTime|CDLUpdatedTime| Id|            Attr3|
        //+-----+--------------+--------------+---+-----------------+
        //|  1_1|             1|             2|  1|    new attribute|
        //| 1_1b|          1000|          2000|  2|another attribute|
        //+-----+--------------+--------------+---+-----------------+
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(templateone).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            switch (id) {
            case 1:
                Assert.assertEquals(record.get("Attr1").toString(), "1_1");
                Assert.assertEquals(record.get("CDLCreatedTime"), 1L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 2L);
                Assert.assertEquals(record.get("Attr3").toString(), "new attribute");
                break;
            case 2:
                Assert.assertEquals(record.get("Attr1").toString(), "1_1b");
                Assert.assertEquals(record.get("CDLCreatedTime"), 1000L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 2000L);
                Assert.assertEquals(record.get("Attr3").toString(), "another attribute");
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifyTemplateTwoSplit(HdfsDataUnit templatetwo) {
        // for template 2, output should be:
        //+-----+-----+-----------------+--------------+--------------+---+-----------------+
        //|Attr1|Attr2|CustomerAccountId|CDLCreatedTime|CDLUpdatedTime| Id|            Attr3|
        //+-----+-----+-----------------+--------------+--------------+---+-----------------+
        //|  2_1|  2_2|              2_3|           100|           200|  1|    new attribute|
        //| 2_1b| 2_2b|             2_3b|           100|           200|  2|another attribute|
        //+-----+-----+-----------------+--------------+--------------+---+-----------------+
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(templatetwo).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            switch (id) {
            case 1:
                Assert.assertEquals(record.get("Attr1").toString(), "2_1");
                Assert.assertEquals(record.get("Attr2").toString(), "2_2");
                Assert.assertEquals(record.get("CustomerAccountId").toString(), "2_3");
                Assert.assertEquals(record.get("CDLCreatedTime"), 100L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 200L);
                Assert.assertEquals(record.get("Attr3").toString(), "new attribute");
                break;
            case 2:
                Assert.assertEquals(record.get("Attr1").toString(), "2_1b");
                Assert.assertEquals(record.get("Attr2").toString(), "2_2b");
                Assert.assertEquals(record.get("CustomerAccountId").toString(), "2_3b");
                Assert.assertEquals(record.get("CDLCreatedTime"), 100L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 200L);
                Assert.assertEquals(record.get("Attr3").toString(), "another attribute");
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifyTemplateTwoWithDiscards(HdfsDataUnit templatetwoWithDiscards) {
        // for template 2 with discard file "Attr3", output should be:
        //+-----+-----+-----------------+--------------+--------------+---+
        //|Attr1|Attr2|CustomerAccountId|CDLCreatedTime|CDLUpdatedTime| Id|
        //+-----+-----+-----------------+--------------+--------------+---+
        //|  2_1|  2_2|              2_3|           100|           200|  1|
        //| 2_1b| 2_2b|             2_3b|           100|           200|  2|
        //+-----+-----+-----------------+--------------+--------------+---+
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(templatetwoWithDiscards).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            switch (id) {
                case 1:
                    Assert.assertEquals(record.get("Attr1").toString(), "2_1");
                    Assert.assertEquals(record.get("Attr2").toString(), "2_2");
                    Assert.assertEquals(record.get("CustomerAccountId").toString(), "2_3");
                    Assert.assertEquals(record.get("CDLCreatedTime"), 100L);
                    Assert.assertEquals(record.get("CDLUpdatedTime"), 200L);
                    Assert.assertEquals(record.get("Attr3"), null);
                    break;
                case 2:
                    Assert.assertEquals(record.get("Attr1").toString(), "2_1b");
                    Assert.assertEquals(record.get("Attr2").toString(), "2_2b");
                    Assert.assertEquals(record.get("CustomerAccountId").toString(), "2_3b");
                    Assert.assertEquals(record.get("CDLCreatedTime"), 100L);
                    Assert.assertEquals(record.get("CDLUpdatedTime"), 200L);
                    Assert.assertEquals(record.get("Attr3"), null);
                    break;
                default:
                    Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifyTemplateThreeSplit(HdfsDataUnit templatetwo) {
        // for template 3, output should be:
        //+-----+-----+--------------+--------------+---+-----------------+
        //|Attr1|Attr2|CDLCreatedTime|CDLUpdatedTime| Id|            Attr3|
        //+-----+-----+--------------+--------------+---+-----------------+
        //|  3_1|  3_2|          1000|          2000|  1|    new attribute|
        //| 3_1b| null|             1|             2|  2|another attribute|
        //+-----+-----+--------------+--------------+---+-----------------+
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(templatetwo).forEachRemaining(record -> {
            int id = (int) record.get("Id");
            switch (id) {
            case 1:
                Assert.assertEquals(record.get("Attr1").toString(), "3_1");
                Assert.assertEquals(record.get("Attr2").toString(), "3_2");
                Assert.assertEquals(record.get("CDLCreatedTime"), 1000L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 2000L);
                Assert.assertEquals(record.get("Attr3").toString(), "new attribute");
                break;
            case 2:
                Assert.assertEquals(record.get("Attr1").toString(), "3_1b");
                Assert.assertEquals(record.get("Attr2"), null);
                Assert.assertEquals(record.get("CDLCreatedTime"), 1L);
                Assert.assertEquals(record.get("CDLUpdatedTime"), 2L);
                Assert.assertEquals(record.get("Attr3").toString(), "another attribute");
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private List<String> prepareData() {
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
                Pair.of("template3__" + InterfaceName.CDLUpdatedTime, Long.class), //
                Pair.of("Attr3", String.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "1_1", 1L, 2L, "2_1", "2_2", "2_3", 100L, 200L, "3_1", "3_2", 1000L, 2000L, "new attribute" }, //
                { 2, "1_1b", 1000L, 2000L, "2_1b", "2_2b", "2_3b", 100L, 200L, "3_1b", null, 1L, 2L,
                        "another attribute" } //
        };
        return data;
    }

}
