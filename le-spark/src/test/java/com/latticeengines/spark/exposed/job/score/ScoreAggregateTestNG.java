package com.latticeengines.spark.exposed.job.score;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ScoreAggregateJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ScoreAggregateTestNG extends SparkJobFunctionalTestNGBase {

    private static final String MODEL_ID = "ModelId";

    @Test(groups = "functional")
    public void test() {
        ExecutorService workers = ThreadPoolUtils.getFixedSizeThreadPool("score-aggregate-test", 2);
        List<Runnable> runnables = new ArrayList<>();
        Runnable runnable1 = () -> testSingleProbability();
        runnables.add(runnable1);
        Runnable runnable2 = () -> testSingleRevenue();
        runnables.add(runnable2);
        Runnable runnable3 = () -> testMultiple();
        runnables.add(runnable3);
        ThreadPoolUtils.runRunnablesInParallel(workers, runnables, 60, 1);
        workers.shutdownNow();

    }

    private void testMultiple() {
        List<String> input = uploadData();
        ScoreAggregateJobConfig config = new ScoreAggregateJobConfig();
        config.expectedValue = false;
        config.modelGuidField = MODEL_ID;
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("Model1", InterfaceName.Probability.name());
        fieldMap.put("Model2", InterfaceName.ExpectedRevenue.name());
        config.scoreFieldMap = fieldMap;

        SparkJobResult result = runSparkJob(ScoreAggregateJob.class, config, input,
                String.format("/tmp/%s/%s/Multiple", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyMultiple));
    }

    private void testSingleProbability() {
        List<String> input = uploadData();
        ScoreAggregateJobConfig config = new ScoreAggregateJobConfig();
        config.expectedValue = false;
        config.modelGuidField = MODEL_ID;

        SparkJobResult result = runSparkJob(ScoreAggregateJob.class, config, input,
                String.format("/tmp/%s/%s/SingleProbability", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySingleProbability));
    }

    private void testSingleRevenue() {
        List<String> input = uploadData();
        ScoreAggregateJobConfig config = new ScoreAggregateJobConfig();
        config.expectedValue = true;
        config.modelGuidField = MODEL_ID;

        SparkJobResult result = runSparkJob(ScoreAggregateJob.class, config, input,
                String.format("/tmp/%s/%s/SingleRevenue", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifySingleRevenue));
    }

    private List<String> uploadData() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(MODEL_ID, String.class), //
                Pair.of(InterfaceName.Probability.name(), Double.class), //
                Pair.of(InterfaceName.ExpectedRevenue.name(), Double.class) //
        );
        Object[][] data = new Object[][] { //
                { "Model1", 100D, 1000D }, //
                { "Model1", 200D, 2000D }, //
                { "Model2", 500D, 5000D }, //
                { "Model2", 200D, 2000D }, //
                { "Model2", 0D, 0D } //
        };
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private Boolean verifyMultiple(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            String modelId = (String) record.get(MODEL_ID).toString();
            if (modelId.equals("Model1")) {
                Assert.assertEquals((Double) record.get(InterfaceName.AverageScore.name()), 150D);
            } else {
                Assert.assertEquals((Double) record.get(InterfaceName.AverageScore.name()), 7000 / 3D);
            }

        });
        Assert.assertEquals(count.get(), 2);
        return true;
    }

    private Boolean verifySingleProbability(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            Assert.assertEquals((Double) record.get(InterfaceName.AverageScore.name()), 200D);

        });
        Assert.assertEquals(count.get(), 1);
        return true;
    }

    private Boolean verifySingleRevenue(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            Assert.assertEquals((Double) record.get(InterfaceName.AverageScore.name()), 2000D);

        });
        Assert.assertEquals(count.get(), 1);
        return true;
    }
}
