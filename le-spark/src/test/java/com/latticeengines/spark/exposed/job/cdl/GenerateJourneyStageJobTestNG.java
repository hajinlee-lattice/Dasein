package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateJourneyStageJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenerateJourneyStageJobTestNG.class);

    // TODO change standard columns

    private static final List<Pair<String, Class<?>>> TIMELINE_FIELDS = Arrays.asList( //
            Pair.of("RecordId", String.class), //
            Pair.of("EventTimestamp", Long.class), //
            Pair.of("AccountId", String.class), //
            Pair.of("ContactId", String.class), //
            Pair.of("EventType", String.class) //
    );

    @Test(groups = "functional")
    private void test() {
        prepareTimelineData();
        JourneyStageJobConfig config = new JourneyStageJobConfig();
        config.masterAccountTimeLineIdx = 0;
        config.diffAccountTimeLineIdx = 1;
        config.currentEpochMilli = System.currentTimeMillis();

        SparkJobResult result = runSparkJob(GenerateJourneyStageJob.class, config);
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verify, this::verify);
    }

    private Boolean verify(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        List<String> verifyColumns = TIMELINE_FIELDS.stream().map(Pair::getKey).collect(Collectors.toList());
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, verifyColumns));
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private void prepareTimelineData() {
        Object[][] masterData = new Object[][] { //
                { "r1", 1L, "a1", "c1", "WebVisit" }, //
                { "r2", 3L, "a1", "c1", "WebVisit" }, //
                { "r4", 5L, "a1", "c1", "WebVisit" }, //
        };

        Object[][] diffData = new Object[][] { //
                { "r5", 1L, "a1", "c1", "WebVisit" }, //
        };

        uploadHdfsDataUnit(masterData, TIMELINE_FIELDS);
        uploadHdfsDataUnit(diffData, TIMELINE_FIELDS);
    }
}
