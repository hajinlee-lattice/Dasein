package com.latticeengines.spark.exposed.job.match;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.LID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.SOURCE_PREFIX;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.latticeengines.spark.exposed.job.match.ParseMatchResultJob;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ParseMatchResultTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        ParseMatchResultJobConfig config = prepareInput();
        SparkJobResult result = runSparkJob(ParseMatchResultJob.class, config);
        verifyResult(result);
    }

    private ParseMatchResultJobConfig prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(SOURCE_PREFIX + "Domain", Integer.class), //
                Pair.of("Name", String.class), //
                Pair.of("City", String.class), //
                Pair.of("State", String.class), //
                Pair.of("Country", String.class), //
                Pair.of(LID_FIELD, Long.class), //
                Pair.of("Domain", String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1, "Name1", "City1", "State1", "Country1", 1L, "d.com" }, //
                { 2, "Name2", "City2", "State2", "Country2", 2L, "d.com" }, //
                { 3, "Name3", "City3", "State3", "Country3", 3L, "d.com" } };
        uploadHdfsDataUnit(data, fields);

        ParseMatchResultJobConfig config = new ParseMatchResultJobConfig();
        config.sourceColumns = Arrays.asList( //
                "Domain", //
                "Name", //
                "City", //
                "State", //
                "Country" //
        );
        config.excludeDataCloudAttrs = true;
        config.joinInternalId = true;
        return config;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            Assert.assertTrue(record.get("Domain") instanceof Integer);
            Assert.assertNull(record.get(LID_FIELD));
        });
        Assert.assertEquals(count.get(), 3);
        return true;
    }

}
