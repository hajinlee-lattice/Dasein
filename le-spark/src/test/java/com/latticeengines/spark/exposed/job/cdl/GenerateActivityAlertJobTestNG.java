package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.DnbIntentData;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.MarketingActivity;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.WebVisit;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AlertData;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AlertName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CreationTimestamp;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PartitionKey;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SortKey;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Title;
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
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
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
            Pair.of(Title.name(), String.class), //
            Pair.of(EventType.getColumnName(), String.class), //
            Pair.of(StreamType.getColumnName(), String.class), //
            Pair.of(TrackedBySystem.getColumnName(), String.class), //
            Pair.of(Detail1.getColumnName(), String.class), //
            Pair.of(Detail2.getColumnName(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> ALERT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(AlertName.name(), String.class), //
            Pair.of(CreationTimestamp.name(), Long.class), //
            Pair.of(AlertData.name(), String.class) //
    );

    @Test(groups = "functional")
    private void test() {
        prepareData();

        ActivityAlertJobConfig config = new ActivityAlertJobConfig();
        Arrays.asList( //
                Alert.INC_WEB_ACTIVITY, Alert.ANONYMOUS_WEB_VISITS, //
                Alert.RE_ENGAGED_ACTIVITY, Alert.HIGH_ENGAGEMENT_IN_ACCOUNT, //
                Alert.ACTIVE_CONTACT_WEB_VISITS, Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES, //
                Alert.RESEARCHING_INTENT_AROUND_PRODUCT_PAGES)
                .forEach(alert -> config.alertNameToQualificationPeriodDays.put(alert, 10L));
        config.currentEpochMilli = 10L;
        config.masterAccountTimeLineIdx = 0;
        config.masterAlertIdx = 1;
        config.dedupAlert = true;

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

            SetUtils.SetView<Pair<String, String>> diffKeys = SetUtils.difference(counts.keySet(),
                    expectedCounts.keySet());
            log.info("Different alerts = {}", diffKeys);
            // make sure we have the expected amount of alerts
            // TODO check more detail than count
            Assert.assertEquals(counts, expectedCounts);
            return true;
        };
    }

    // TODO enhance test cases when requirement detail is clear
    private void prepareData() {
        Object[][] timelineMasterData = new Object[][] { //
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
                 * re-engaged contacts (one of them have two contacts, the other only have one)
                 */
                newMARecord("a8", "c1", -30000000000L), //
                newMARecord("a8", "c1", 0L), //
                newMARecord("a8", "c2", -30000000000L), //
                newMARecord("a8", "c2", 0L), //
                newMARecord("a9", "c1", -30000000000L), //
                newMARecord("a9", "c1", 0L), //

                // interval between two events are too short, not consider as re-engaged
                newMARecord("a10", "c1", -10000L), //
                newMARecord("a10", "c1", 0L), //

                /*-
                 * from researching stage to buying stage
                 */
                newIntentRecord("a3", 2L, "m1", "0.49"), //
                newIntentRecord("a3", 3L, "m2", "0.55"), //
                /*-
                 * first intent is in researching stage
                 */
                newIntentRecord("a4", 3L, "m1", "0.45"), //
                /*-
                 * first intent is in buying stage => growing buying intent alert
                 */
                newIntentRecord("a5", 3L, "m3", "0.90"), //
                /*-
                 * latest two intents are both in buying stage => no alert
                 */
                newIntentRecord("a6", 2L, "m2", "0.80"), //
                newIntentRecord("a6", 3L, "m2", "0.90"), //
                /*-
                 * latest intent has invalid buying score => no alert
                 */
                newIntentRecord("a7", 3L, "m3", "abc"), //
                /*-
                 * latest two intents are both in researching stage => no alert
                 */
                newIntentRecord("a8", 2L, "m1", "0.45"), //
                newIntentRecord("a8", 3L, "m4", "0.46"), //

                // no. of marketing activity > 5 => High Engagement In Account
                newMARecord("a11", "c1", 2L), //
                newMARecord("a11", "c2", 3L), //
                newMARecord("a11", "c3", 4L), //
                newMARecord("a11", "c4", 1L), //
                newMARecord("a11", "c5", 0L), //
                newMARecord("a11", "c6", 6L), //

                // a12 has contacts but no MA => a12 won't have Active Contacts and Web Visits
                // alert
                newTimelineContactRecord("a12", "c1", "Manager", WebVisit, "Page group 1",
                        "Data Management product, Business Intelligence"), //
                newTimelineContactRecord("a12", "c2", "Software Engineer", WebVisit, "Page group 2", "AI products"), //
                newTimelineContactRecord("a13", "c1", "QA", WebVisit, "Page group 2", "AI products"), //
                newTimelineContactRecord("a13", "c1", "QA", WebVisit, "Page group 3", "DB products"), //
                newTimelineContactRecord("a13", "c2", "Product Manager", WebVisit, "Page group 4",
                        "Marketing products"), //
                // a13 has contacts and MA => a13 has Active Contacts and Web Visits alert
                newMARecord("a13", "c3", 2L), //
        };
        uploadHdfsDataUnit(timelineMasterData, TIMELINE_FIELDS);

        Object[][] alertMasterData = new Object[][] { //
                /*-
                 * duplicate alert that will be not be generated again
                 */
                { "a1", Alert.ANONYMOUS_WEB_VISITS, 10L,
                        "{\"Data\":{\"PageVisits\":2},\"StartTimestamp\":-863999990,\"EndTimestamp\":10}" }, //
                /*-
                 * no duplicate
                 */
                { "a100", Alert.ANONYMOUS_WEB_VISITS, 10L, "{}" }, //
        };
        uploadHdfsDataUnit(alertMasterData, ALERT_FIELDS);
    }

    // [ account id, alert name ] => count
    private Map<Pair<String, String>, Integer> getExpectedAlertCount() {
        Map<Pair<String, String>, Integer> counts = new HashMap<>();

        counts.put(Pair.of("a12", Alert.INC_WEB_ACTIVITY), 1);
        counts.put(Pair.of("a13", Alert.INC_WEB_ACTIVITY), 1);

        counts.put(Pair.of("a1", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a2", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a3", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a4", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a5", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a6", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a7", Alert.ANONYMOUS_WEB_VISITS), 1);
        counts.put(Pair.of("a100", Alert.ANONYMOUS_WEB_VISITS), 1); // from existing alert table

        counts.put(Pair.of("a8", Alert.RE_ENGAGED_ACTIVITY), 1);
        counts.put(Pair.of("a9", Alert.RE_ENGAGED_ACTIVITY), 1);

        counts.put(Pair.of("a11", Alert.HIGH_ENGAGEMENT_IN_ACCOUNT), 1);

        counts.put(Pair.of("a8", Alert.ACTIVE_CONTACT_WEB_VISITS), 1);
        counts.put(Pair.of("a9", Alert.ACTIVE_CONTACT_WEB_VISITS), 1);
        counts.put(Pair.of("a10", Alert.ACTIVE_CONTACT_WEB_VISITS), 1);
        counts.put(Pair.of("a11", Alert.ACTIVE_CONTACT_WEB_VISITS), 1);
        counts.put(Pair.of("a13", Alert.ACTIVE_CONTACT_WEB_VISITS), 1);

        counts.put(Pair.of("a3", Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES), 1);
        counts.put(Pair.of("a5", Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES), 1);
        counts.put(Pair.of("a6", Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES), 1);

        counts.put(Pair.of("a4", Alert.RESEARCHING_INTENT_AROUND_PRODUCT_PAGES), 1);
        return counts;
    }

    private Object[] newMARecord(String accountId, String contactId, long timestamp) {
        return new Object[] { //
                "p", "s", "r", timestamp, // not testing pk, sk, uuid and timestamp for now
                accountId, contactId, null, "fake event", MarketingActivity.name(), null, null, null, //
        };
    }

    private Object[] newIntentRecord(String accountId, long timestamp, String detail1, String detail2) {
        return new Object[] { //
                "p", "s", "r", timestamp, // not testing pk, sk, uuid and timestamp for now
                accountId, null, null, "fake event", DnbIntentData.name(), null, detail1, detail2, //
        };
    }

    private Object[] newTimelineRecord(String accountId, AtlasStream.StreamType type, String detail1, String detail2) {
        return new Object[] { //
                "p", "s", "r", 0L, // not testing pk, sk, uuid and timestamp for now
                accountId, null, null, "fake event", type == null ? null : type.name(), null, detail1, detail2, //
        };
    }

    private Object[] newTimelineContactRecord(String accountId, String contactId, String title,
            AtlasStream.StreamType type, String detail1, String detail2) {
        Object[] row = newTimelineRecord(accountId, type, detail1, detail2);
        row[5] = contactId;
        row[6] = title;
        return row;
    }
}
