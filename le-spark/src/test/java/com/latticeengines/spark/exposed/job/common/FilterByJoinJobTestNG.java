package com.latticeengines.spark.exposed.job.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class FilterByJoinJobTestNG extends SparkJobFunctionalTestNGBase {
    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testInnerJoin);
        runnables.add(this::testOuterJoin);
        runnables.add(this::testLeftAntiJoin);
        runnables.add(this::testSwitchSide);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testInnerJoin() {
        List<String> input = prepareData();
        FilterByJoinConfig config = getInnerJoinConfig();
        SparkJobResult result = runSparkJob(FilterByJoinJob.class, config, input,
                String.format("/tmp/%s/%s/testInnerJoin", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyInnerJoin));
    }

    private FilterByJoinConfig getInnerJoinConfig() {
        FilterByJoinConfig config = new FilterByJoinConfig();
        config.setKey("Id");
        config.setJoinType("inner");
        return config;
    }

    private Boolean verifyInnerJoin(HdfsDataUnit innerJoinResults) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(innerJoinResults).forEachRemaining(record -> {
            //System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 4);
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 3L);
        return true;
    }

    private void testOuterJoin() {
        List<String> input = prepareData();
        FilterByJoinConfig config = getOuterJoinConfig();
        SparkJobResult result = runSparkJob(FilterByJoinJob.class, config, input,
                String.format("/tmp/%s/%s/testOuterJoin", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyOuterJoin));
    }

    private FilterByJoinConfig getOuterJoinConfig() {
        FilterByJoinConfig config = new FilterByJoinConfig();
        config.setKey("Id");
        config.setJoinType("outer");
        return config;
    }

    private Boolean verifyOuterJoin(HdfsDataUnit outerJoinResults) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(outerJoinResults).forEachRemaining(record -> {
            //System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 4);
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 5L);
        return true;
    }

    private void testLeftAntiJoin() {
        List<String> input = prepareData();
        FilterByJoinConfig config = getLeftAntiJoinConfig();
        SparkJobResult result = runSparkJob(FilterByJoinJob.class, config, input,
                String.format("/tmp/%s/%s/testLeftAntiJoin", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyLeftAntiJoin));
    }

    private FilterByJoinConfig getLeftAntiJoinConfig() {
        FilterByJoinConfig config = new FilterByJoinConfig();
        config.setKey("Id");
        config.setJoinType("left_anti");
        config.setSelectColumns(Arrays.asList("Id", "Attr1"));
        return config;
    }

    private Boolean verifyLeftAntiJoin(HdfsDataUnit leftantiResults) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(leftantiResults).forEachRemaining(record -> {
            //System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 2);
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 1L);
        return true;
    }

    private List<String> prepareData() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> sourceFields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Integer.class) //
        );
        Object[][] sourceData = new Object[][] { //
                { 1, "1_1", 1L, 1 }, //
                { 2, "2_1", null, 2 }, //
                { 3, null, 3L, 3 }, //
                { 4, "4_1", 4L, 4 }, //
        };
        input.add(uploadHdfsDataUnit(sourceData, sourceFields));

        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr5", Long.class), //
                Pair.of("Attr6", Integer.class), //
                Pair.of("Attr7", String.class) //
        );
        Object[][] inputData = new Object[][] { //
                { 1, "1_1", 1L, 1, "1_7" }, //
                { 2, "2_1", null, 2, "2_7" }, //
                { 4, "4_1", 4L, 4, "4_7" }, //
                { 5, "5_1", 5L, 5, "5_7" }, //
        };
        input.add(uploadHdfsDataUnit(inputData, inputFields));
        return input;
    }

    private void testSwitchSide() {
        List<String> input = prepareData();
        FilterByJoinConfig config = getSwitchSideConfig();
        SparkJobResult result = runSparkJob(FilterByJoinJob.class, config, input,
                String.format("/tmp/%s/%s/testSwitchSide", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySwitchSide));
    }

    private FilterByJoinConfig getSwitchSideConfig() {
        FilterByJoinConfig config = new FilterByJoinConfig();
        config.setJoinType("left_anti");
        config.setSelectColumns(Arrays.asList("Id", "Attr1"));
        config.setSwitchSide(true);
        config.setKey("Id");
        return config;
    }

    private Boolean verifySwitchSide(HdfsDataUnit switchSideResults) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(switchSideResults).forEachRemaining(record -> {
            System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 2);
            count.addAndGet(1);
            Assert.assertEquals("5", record.get("Id").toString());
        });
        Assert.assertEquals(count.get(), 1L);
        return true;
    }
}
