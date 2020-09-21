package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.GROWING_BUYER_INTENT;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.GROWING_RESEARCH_INTENT;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.INC_WEB_ACTIVITY_ON_PRODUCT;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.DnbIntentData;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.WebVisit;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AlertData;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AlertName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PartitionKey;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SortKey;
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
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityAlertJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateActivityAlertJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateActivityAlertJobTestNG.class);

    private static final List<Pair<String, Class<?>>> TIMELINE_FIELDS = Arrays.asList( //
            Pair.of(PartitionKey.name(), String.class), //
            Pair.of(SortKey.name(), String.class), //
            Pair.of(RecordId.getColumnName(), String.class), //
            Pair.of(EventDate.getColumnName(), Long.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(EventType.getColumnName(), String.class), //
            Pair.of(StreamType.getColumnName(), String.class), //
            Pair.of(TrackedBySystem.getColumnName(), String.class), //
            Pair.of(Detail1.getColumnName(), String.class), //
            Pair.of(Detail2.getColumnName(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> ALERT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(AlertName.name(), String.class), //
            Pair.of(AlertData.name(), String.class) //
    );

    @Test(groups = "functional")
    private void test() {
        prepareData();

        ActivityAlertJobConfig config = new ActivityAlertJobConfig();
        config.alertNameToQualificationPeriodDays.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY, 10L);
        config.alertNameToQualificationPeriodDays.put(INC_WEB_ACTIVITY_ON_PRODUCT, 10L);
        config.alertNameToQualificationPeriodDays.put(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY, 10L);
        config.alertNameToQualificationPeriodDays.put(ActivityStoreConstants.Alert.GROWING_BUYER_INTENT, 10L);
        config.alertNameToQualificationPeriodDays.put(ActivityStoreConstants.Alert.GROWING_RESEARCH_INTENT, 10L);
        config.currentEpochMilli = 10L;
        config.masterAccountTimeLineIdx = 0;

        SparkJobResult result = runSparkJob(GenerateActivityAlertJob.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(getDebugFn("Master"), (tgt) -> true);
    }

    private Function<HdfsDataUnit, Boolean> getDebugFn(String name) {
        return (tgt) -> {
            AtomicInteger counter = new AtomicInteger(0);
            List<GenericRecord> records = new ArrayList<>();

            Map<Pair<String, String>, Integer> expectedCounts = getExpectedAlertCount();
            Map<Pair<String, String>, Integer> counts = new HashMap<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                Assert.assertNotNull(record.get(AccountId.name()),
                        String.format("found null account id in record %s", record.toString()));
                counter.incrementAndGet();
                records.add(record);

                String accountId = record.get(AccountId.name()).toString();
                String alertName = record.get(AlertName.name()).toString();
                Pair<String, String> key = Pair.of(accountId, alertName);
                counts.put(key, counts.getOrDefault(key, 0) + 1);
            });
            log.info("Alert Output:");
            List<String> cols = ALERT_FIELDS.stream().map(Pair::getKey).collect(Collectors.toList());
            records.forEach(record -> log.info(name + ": " + debugStr(record, cols)));
            log.info("Alert count: {}", counts);

            // make sure we have the expected amount of alerts
            // TODO check more detail than count
            Assert.assertEquals(counts, expectedCounts);
            return true;
        };
    }

    // TODO enhance test cases when requirement detail is clear
    private void prepareData() {
        Object[][] masterData = new Object[][] { //
                newTimelineRecord("a2", WebVisit, "Page 1", "page 1,page 2,page 3"), //
                newTimelineRecord("a2", WebVisit, "Page 1", "page 3"), //
                newTimelineRecord("a2", WebVisit, "Page 1", "page 1,page 2"), //
                newTimelineRecord("a2", WebVisit, "Page 1", "page 1"), //
                newTimelineRecord("a1", WebVisit, "Page 1", "page 4"), //
                newTimelineRecord("a1", WebVisit, "Page 1", "page 1,page 4"), //

                newTimelineRecord("a3", WebVisit, "Page 1", "page 1,page 4"), //
                newTimelineRecord("a4", WebVisit, "Page 1", "page 1,page 4"), //
                newTimelineRecord("a5", WebVisit, "Page 1", "page 1,page 4"), //
                newTimelineRecord("a6", WebVisit, "Page 1", "page 1,page 4"), //
                newTimelineRecord("a7", WebVisit, "Page 1", "page 1,page 4"), //

                /*-
                 * re-visit page 1 after over 30days
                 */
                newWebVisitRecord("a8", -30000000000L, "Page 1", "page 1,page 2"), //
                newTimelineRecord("a8", WebVisit, "Page 1", "page 1,page 4"), //

                /*-
                 * from researching stage to buying stage
                 */
                newIntentRecord("a3", 2L, "0.88", "0.49"), //
                newIntentRecord("a3", 3L, "0.88", "0.55"), //
                /*-
                 * first intent is in researching stage
                 */
                newIntentRecord("a4", 3L, "0.88", "0.45"), //
                /*-
                 * first intent is in buying stage => growing buying intent alert
                 */
                newIntentRecord("a5", 3L, "0.88", "0.90"), //
                /*-
                 * latest two intents are both in buying stage => no alert
                 */
                newIntentRecord("a6", 2L, "0.88", "0.80"), //
                newIntentRecord("a6", 3L, "0.88", "0.90"), //
                /*-
                 * latest intent has invalid buying score => no alert
                 */
                newIntentRecord("a7", 3L, "0.88", "abc"), //
                /*-
                 * latest two intents are both in researching stage => no alert
                 */
                newIntentRecord("a8", 2L, "0.88", "0.45"), //
                newIntentRecord("a8", 3L, "0.88", "0.46"), //
        };
        uploadHdfsDataUnit(masterData, TIMELINE_FIELDS);
    }

    // [ account id, alert name ] => count
    private Map<Pair<String, String>, Integer> getExpectedAlertCount() {
        Map<Pair<String, String>, Integer> counts = new HashMap<>();

        counts.put(Pair.of("a1", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a2", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a3", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a4", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a5", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a6", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a7", INC_WEB_ACTIVITY_ON_PRODUCT), 1);
        counts.put(Pair.of("a8", INC_WEB_ACTIVITY_ON_PRODUCT), 1);

        counts.put(Pair.of("a8", RE_ENGAGED_ACTIVITY), 1);

        counts.put(Pair.of("a4", GROWING_RESEARCH_INTENT), 1);
        counts.put(Pair.of("a3", GROWING_BUYER_INTENT), 1);
        counts.put(Pair.of("a5", GROWING_BUYER_INTENT), 1);

        return counts;
    }

    private Object[] newWebVisitRecord(String accountId, long timestamp, String detail1, String detail2) {
        return new Object[] { //
                "p", "s", "r", timestamp, // not testing pk, sk, uuid and timestamp for now
                accountId, null, "fake event", WebVisit.name(), null, detail1, detail2, //
        };
    }

    private Object[] newIntentRecord(String accountId, long timestamp, String detail1, String detail2) {
        return new Object[] { //
                "p", "s", "r", timestamp, // not testing pk, sk, uuid and timestamp for now
                accountId, null, "fake event", DnbIntentData.name(), null, detail1, detail2, //
        };
    }

    private Object[] newTimelineRecord(String accountId, AtlasStream.StreamType type, String detail1, String detail2) {
        return new Object[] { //
                "p", "s", "r", 0L, // not testing pk, sk, uuid and timestamp for now
                accountId, null, "fake event", type == null ? null : type.name(), null, detail1, detail2, //
        };
    }
}
