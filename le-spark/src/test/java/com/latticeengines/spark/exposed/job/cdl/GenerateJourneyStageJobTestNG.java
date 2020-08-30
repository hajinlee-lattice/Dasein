package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.DnbIntentData;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.MarketingActivity;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.Opportunity;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.WebVisit;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PartitionKey;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SortKey;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageName;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.Detail1;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.Detail2;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.EventDate;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.EventType;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.RecordId;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.StreamType;
import static com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn.TrackedBySystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStageUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig;
import com.latticeengines.domain.exposed.util.TypeConversionUtil;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateJourneyStageJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenerateJourneyStageJobTestNG.class);

    private static final Long FAKE_CURRENT_TIME = 5L;
    private static final String JOURNEY_STAGE_EVENT_TYPE = "Journey Stage Change";

    private static final List<Pair<String, Class<?>>> TIMELINE_FIELDS = Arrays.asList( //
            Pair.of(PartitionKey.name(), String.class), //
            Pair.of(SortKey.name(), String.class), //
            Pair.of(RecordId.getColumnName(), String.class), //
            Pair.of(EventDate.getColumnName(), Long.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(EventType.getColumnName(), String.class), //
            Pair.of(StreamType.getColumnName(), String.class), //
            Pair.of(TrackedBySystem.getColumnName(), String.class), //
            Pair.of(Detail1.getColumnName(), String.class), //
            Pair.of(Detail2.getColumnName(), String.class) //
    );
    private static final List<Pair<String, Class<?>>> JOURNEY_STAGE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(InterfaceName.StageName.name(), String.class), //
            Pair.of(EventDate.getColumnName(), Long.class) //
    );

    @Test(groups = "functional")
    private void testBaseCase() {
        prepareTimelineData();
        JourneyStageJobConfig config = new JourneyStageJobConfig();
        config.masterAccountTimeLineIdx = 0;
        config.diffAccountTimeLineIdx = 1;
        config.masterJourneyStageIdx = 2;
        config.currentEpochMilli = FAKE_CURRENT_TIME;

        List<JourneyStage> stages = JourneyStageUtils.atlasJourneyStages(null);
        config.journeyStages = new ArrayList<>(stages.subList(0, stages.size() - 1));
        config.defaultStage = stages.get(stages.size() - 1);
        config.accountTimeLineVersion = "v1";
        config.accountTimeLineId = "account360_timeline";

        SparkJobResult result = runSparkJob(GenerateJourneyStageJob.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(verifyTimeLineFn("master", 18, 6), verifyTimeLineFn("diff", 7, 6),
                verifyJourneyStageFn(getExpectedStageNames()));
    }

    private Function<HdfsDataUnit, Boolean> verifyTimeLineFn(String name, int expectedNumRecords,
            int expectedNumJourneyStageRecords) {
        return (tgt) -> {
            AtomicInteger counter = new AtomicInteger(0);
            AtomicInteger journeyStageCounter = new AtomicInteger(0);
            List<GenericRecord> records = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                Assert.assertNotNull(record.get(AccountId.name()),
                        String.format("found null account id in record %s", record.toString()));
                String eventType = TypeConversionUtil.toString(record.get(EventType.getColumnName()));
                if (JOURNEY_STAGE_EVENT_TYPE.equals(eventType)) {
                    journeyStageCounter.incrementAndGet();
                    String streamType = TypeConversionUtil.toString(record.get(StreamType.getColumnName()));
                    Assert.assertEquals(streamType, AtlasStream.StreamType.JourneyStage.name());
                    Assert.assertNotNull(record.get(Detail1.getColumnName()));
                }
                counter.incrementAndGet();
                records.add(record);
            });
            log.info("TimeLine Output:");
            List<String> cols = TIMELINE_FIELDS.stream().map(Pair::getKey).collect(Collectors.toList());
            records.forEach(record -> log.info(name + ": " + debugStr(record, cols)));

            Assert.assertEquals(counter.get(), expectedNumRecords);
            Assert.assertEquals(journeyStageCounter.get(), expectedNumJourneyStageRecords);
            return true;
        };
    }

    private Function<HdfsDataUnit, Boolean> verifyJourneyStageFn(@NotNull Map<String, String> expectedStages) {
        return (tgt) -> {
            Map<String, String> stageNames = new HashMap<>();
            AtomicInteger counter = new AtomicInteger(0);
            List<GenericRecord> records = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                String accountId = TypeConversionUtil.toString(record.get(AccountId.name()));
                String stageName = TypeConversionUtil.toString(record.get(StageName.name()));
                Assert.assertNotNull(accountId, String.format("found null account id in record %s", record.toString()));
                Assert.assertNotNull(stageName, String.format("found null stage name in record %s", record.toString()));
                stageNames.put(accountId, stageName);
                counter.incrementAndGet();
                records.add(record);
            });

            log.info("Journey Stage Output:");
            List<String> cols = JOURNEY_STAGE_FIELDS.stream().map(Pair::getKey).collect(Collectors.toList());
            records.forEach(record -> log.info(debugStr(record, cols)));

            Assert.assertEquals(stageNames, expectedStages, "stage name for accounts do not match the expected values");
            return true;
        };
    }

    private void prepareTimelineData() {
        // pk, sk, record id, time, account id, event type, stream type, source,
        // detail1, detail2
        Object[][] masterData = new Object[][] { //
                /*-
                 * accounts that only qualify for one stage
                 */
                newTimelineRecord("a1", Opportunity, "Closed Won"), // no existing stage
                newTimelineRecord("a2", WebVisit, "Page 1"), // same as existing stage
                newTimelineRecord("a3", Opportunity, "Open"), // diff as existing stage
                /*-
                 * accounts that qualify for more than one stages
                 */
                newTimelineRecord("a4", DnbIntentData, "Model1"), // no existing stage
                newTimelineRecord("a4", WebVisit, "Page 2"), // Engaged has higher priority

                newTimelineRecord("a5", Opportunity, "Closed"), // same as existing stage
                newTimelineRecord("a5", Opportunity, "Open"), //
                newTimelineRecord("a5", DnbIntentData, "Model1"), //

                newTimelineRecord("a6", MarketingActivity, "Email Click"), // diff as existing stage
                newTimelineRecord("a6", DnbIntentData, "Model1"), //
                newTimelineRecord("a6", Opportunity, "Closed Won"), //

                oldTimelineRecord("a8", WebVisit, "Page 1"), // old visit, no existing stage

                /*-
                 * accounts with no current records (qualify for default stage): a8
                 */
        };

        Object[][] diffData = new Object[][] { //
                newTimelineRecord("a1", Opportunity, "Closed"), //
        };
        Object[][] journeyStageData = new Object[][] { //
                { "a2", "Engaged", 0L }, //
                { "a3", "Closed", 0L }, //
                { "a5", "Closed", 0L }, //
                { "a6", "Known Engaged", 0L }, //
                { "a7", "Aware", 0L }, //
                /*-
                 * no existing stage for a8, generate default
                 */
        };

        uploadHdfsDataUnit(masterData, TIMELINE_FIELDS);
        uploadHdfsDataUnit(diffData, TIMELINE_FIELDS);
        uploadHdfsDataUnit(journeyStageData, JOURNEY_STAGE_FIELDS);
    }

    private Map<String, String> getExpectedStageNames() {
        Map<String, String> stageNames = new HashMap<>();
        stageNames.put("a1", "Closed-Won");
        stageNames.put("a2", "Engaged");
        stageNames.put("a3", "Opportunity");
        stageNames.put("a4", "Engaged");
        stageNames.put("a5", "Closed");
        stageNames.put("a6", "Closed-Won");
        stageNames.put("a7", "Dark");
        stageNames.put("a8", "Dark");
        return stageNames;
    }

    private Object[] oldTimelineRecord(String accountId, AtlasStream.StreamType type, String detail1) {
        Object[] row = newTimelineRecord(accountId, type, detail1);
        row[3] = Long.MIN_VALUE;
        return row;
    }

    private Object[] newTimelineRecord(String accountId, AtlasStream.StreamType type, String detail1) {
        return new Object[] { //
                "p", "s", "r", 0L, // not testing pk, sk, uuid and timestamp for now
                accountId, "fake event", type == null ? null : type.name(), null, detail1, null, //
        };
    }
}
