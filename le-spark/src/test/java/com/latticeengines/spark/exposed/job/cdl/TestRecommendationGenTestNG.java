package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class TestRecommendationGenTestNG extends TestJoinTestNGBase {

    @Test(groups = "functional")
    public void runTest() {
        uploadInputAvro();
        CreateRecommendationConfig createRecConfig = new CreateRecommendationConfig();
        PlayLaunchSparkContext playLaunchContext = new PlayLaunchSparkContext();
        playLaunchContext.setJoinKey("Field1");
        createRecConfig.setPlayLaunchSparkContext(playLaunchContext);
        SparkJobResult result = runSparkJob(JoinJob.class, createRecConfig);
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is my recommendation!");
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyOutput);
    }

    private Boolean verifyOutput(HdfsDataUnit target) {
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

    @Override
    protected void uploadInputAvro() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Long.class), //
                Pair.of("Field1", String.class), //
                Pair.of("Field2", Integer.class) //
        );
        Object[][] data = new Object[][] { //
                { 0L, "1", 1 }, //
                { 1L, "1", 2 }, //
                { 2L, "2", 3 }, //
        };
        String data1 = uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Long.class), //
                Pair.of("Field1", String.class), //
                Pair.of("Field2", Integer.class) //
        );
        data = new Object[][] { //
                { 0L, "1", 1 }, //
                { 1L, "2", 2 }, //
                { 2L, "3", 3 }, //
        };
        String data2 = uploadHdfsDataUnit(data, fields);

        inputs = Arrays.asList(data2, data1);
    }

}
