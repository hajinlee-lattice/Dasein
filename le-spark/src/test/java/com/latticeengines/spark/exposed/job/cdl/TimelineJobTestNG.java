package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.BuyingScore;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLTemplateName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.InternalId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StreamDateId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__Row_Count__;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TimeLineJobConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class TimelineJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TimelineJobTestNG.class);

    private static final String DATE_ATTR = InterfaceName.WebVisitDate.name();
    private static final String OPP_ID = "OpportunityId";
    private static final String Stage = "Stage";
    private static final String PARTITION_KEY = "partitionKey";
    private static final String SORT_KEY = "sortKey";
    private static final String ALL_CTN_PAGE_PTN_NAME = "all content pages";
    private static final String ALL_CTN_PAGE_PTN_HASH = DimensionGenerator.hashDimensionValue(ALL_CTN_PAGE_PTN_NAME);
    private static final String ALL_CTN_PAGE_PTN_ID = "1";
    private static final String VIDEO_CTN_PAGE_PTN_NAME = "all video content pages";
    private static final String VIDEO_CTN_PAGE_PTN_HASH = DimensionGenerator
            .hashDimensionValue(VIDEO_CTN_PAGE_PTN_NAME);
    private static final String VIDEO_CTN_PAGE_PTN_ID = "2";
    private static final String GOOGLE_PAID_SRC = "Google/Paid";
    private static final String GOOGLE_PAID_SRC_HASH = DimensionGenerator.hashDimensionValue(GOOGLE_PAID_SRC);
    private static final String GOOGLE_PAID_SRC_ID = "3";
    private static final String GOOGLE_ORGANIC_SRC = "Google/Organic";
    private static final String GOOGLE_ORGANIC_SRC_HASH = DimensionGenerator.hashDimensionValue(GOOGLE_ORGANIC_SRC);
    private static final String GOOGLE_ORGANIC_SRC_ID = "4";
    private static final String FACEBOOK_PAID_SRC = "Facebook/Paid";
    private static final String FACEBOOK_PAID_SRC_HASH = DimensionGenerator.hashDimensionValue(FACEBOOK_PAID_SRC);
    private static final String FACEBOOK_PAID_SRC_ID = "5";
    private static final Map<String, String> WEBVISIT_DIMENSION_HASH_ID_MAP = new HashMap<>();

    private static final List<Pair<String, Class<?>>> WEB_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(EntityId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(DATE_ATTR, Long.class), //
            Pair.of(__StreamDate.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(CDLTemplateName.name(), String.class)
    );

    private static final List<Pair<String, Class<?>>> OPP_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(OPP_ID, String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(Stage, String.class), //
            Pair.of(LastModifiedDate.name(), Long.class), //
            Pair.of(__StreamDate.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(BuyingScore.name(), Double.class), //
            Pair.of(CDLTemplateName.name(), String.class));

    private static final List<Pair<String, Class<?>>> CTK_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ActivityType.name(), String.class), //
            Pair.of(ActivityDate.name(), Long.class), //
            Pair.of(__StreamDate.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(__Row_Count__.name(), Long.class), //
            Pair.of(CDLTemplateName.name(), String.class)
    );
    private static final List<Pair<String, Class<?>>> CTK_TABLE_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ContactName.name(), String.class), //
            Pair.of(PhoneNumber.name(), String.class) //
    );

    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    private Map<String, Integer> rawStreamInputIdx = new HashMap<>();
    private Integer contactTableIdx;
    private Map<String, String> streamTypeWithTableNameMap = new HashMap<>();
    private Map<String, Map<String, Set<String>>> timelineRelatedStreamTables = new HashMap<>();
    private Map<String, TimeLine> timeLineMap = new HashMap<>();
    private TimeLine timeLine1;
    private TimeLine timeLine2;
    private TimeLine timeLine3;
    private Map<String, String> templateToSystemTypeMap = new HashMap<>();
    private Map<String, String> timelineVersionMap = new HashMap<>();
    private Map<String, String> tableNameToStreamIdMap = new HashMap<>();
    private Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap = new HashMap<>();

    static {
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(ALL_CTN_PAGE_PTN_HASH, ALL_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(VIDEO_CTN_PAGE_PTN_HASH, VIDEO_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_PAID_SRC_HASH, GOOGLE_PAID_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_ORGANIC_SRC_HASH, GOOGLE_ORGANIC_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(FACEBOOK_PAID_SRC_HASH, FACEBOOK_PAID_SRC_ID);
    }

    @Test(groups = "functional")
    private void test() {
        preparetimeline1();
        preparetimeline2();
        preparetimeline3();
        prepareData();

        SparkJobResult result = runSparkJob(TimeLineJob.class, baseConfig());
        log.info("result is {}.", result.getTargets().stream().map(HdfsDataUnit::getPath).collect(Collectors.toList()));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verify, this::verify, this::verify, this::verify, this::verify, this::verify);
    }

    private Boolean verify(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        List<String> verifyColumns = new ArrayList<>(TimeLineStoreUtils.TimelineStandardColumn.getColumnNames());
        verifyColumns.add(PARTITION_KEY);
        verifyColumns.add(SORT_KEY);
        Map<String, List<String>> expectedDetail2Map = prepareExpectedDetail2Result();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            log.info(debugStr(record, verifyColumns));
            Object detail1 = record.get(InterfaceName.Detail1.name());
            if (detail1 != null) {
                List<String> expectedDetail2 = expectedDetail2Map.get(detail1.toString());
                String detail2 = record.get(InterfaceName.Detail2.name()).toString();
                List<String> detail2Arr = Arrays.asList(detail2.split(","));
                log.info("detail1 is {}, detail2 is {}.", detail1.toString(), detail2);
                Assert.assertEquals(detail2Arr, expectedDetail2);
            }
            counter.incrementAndGet();
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private void prepareData() {
        // ContactId, AccountId, ActivityType, ____StreamDate, StreamDateId, __Row_Count__
        // AccountId is set to junk values intentionally to test contact batch store
        Object[][] importData = new Object[][]{ //
                testCTKRow("C1", "sldkfjkls", "Email Sent", 1L, 0), // last activity date for a1, c1
                testCTKRow("C1", "dfdfdfd", "Email Sent", 1L, 2), //
                testCTKRow("C1", "dfksjld", "Email Sent", 2L, 3), //
                testCTKRow("C1", "dfksjld", "Email Sent", 2L, 5), //
                testCTKRow("C2", "dfksjld", "Email Clicked", 500L, 4), //
                testCTKRow("C2", "dfksjld", "Form Filled", 1005L, 3), //
                testCTKRow("C2", "dfksjld", "Email Sent", 9876L, 2), //
                testCTKRow("C3", "dfksjld", "Email Sent", 8316L, 8), //
                testCTKRow("C3", "dfksjld", "Form Filled", 2L, 1), //
                testCTKRow("C4", "dfksjld", "Email Sent", 18L, 9), //
        };
        String ctkTableName = uploadHdfsDataUnit(importData, CTK_STREAM_IMPORT_FIELDS);
        rawStreamInputIdx.put(ctkTableName, 0);
        streamTypeWithTableNameMap.put(ctkTableName, AtlasStream.StreamType.MarketingActivity.name());
        tableNameToStreamIdMap.put(ctkTableName, "maketing_00q1");

        Object[][] importWeb = new Object[][]{ //
                testWebRow(100L, 1, "https://dnb.com/contents/audios/1"), //
                testWebRow(101L, 8, "https://dnb.com/contents/videos/1"), //
                testWebRow(102L, 5, "https://dnb.com/contents/videos/2"), //
                testWebRow(103L, 7, "https://dnb.com/contents/audios/5"), //
        };
        String webTableName = uploadHdfsDataUnit(importWeb, WEB_STREAM_IMPORT_FIELDS);
        rawStreamInputIdx.put(webTableName, 1);
        streamTypeWithTableNameMap.put(webTableName, AtlasStream.StreamType.WebVisit.name());
        String streamId = "web_00q1";
        tableNameToStreamIdMap.put(webTableName, streamId);
        dimensionMetadataMap.put(streamId, webVisitMetadata());

        Object[][] inputOpp = new Object[][]{ //
                testOppRow(111L, "opp1", "open", 0),
                testOppRow(122L, "opp1", "dev", 2),
                testOppRow(123L, "opp1", "won", 3),
                testOppRow(124L, "opp2", "open", 1)
        };
        String oppTableName = uploadHdfsDataUnit(inputOpp, OPP_STREAM_IMPORT_FIELDS);
        rawStreamInputIdx.put(oppTableName, 2);
        streamTypeWithTableNameMap.put(oppTableName, AtlasStream.StreamType.Opportunity.name());
        tableNameToStreamIdMap.put(oppTableName, "opp_00q1");

        // ContactId, AccountId, ContactName, PhoneNumber
        Object[][] ctkBatchStore = new Object[][]{ //
                {"C1", "A1", "john doe", "(000)-000-0000"}, //
                {"C2", "A1", "jane doe", "(000)-000-0000"}, //
                {"C3", "A2", "tourist", "(000)-000-0000"}, //
                {"C4", "A3", "hello world", "(000)-000-0000"}, //
        };
        uploadHdfsDataUnit(ctkBatchStore, CTK_TABLE_FIELDS);
        contactTableIdx = 3;


        Map<String, Set<String>> timeline1RelatedStreamTables = new HashMap<>();
        timeline1RelatedStreamTables.put(BusinessEntity.Contact.name(), Collections.singleton(ctkTableName));
        timeline1RelatedStreamTables.put(BusinessEntity.Account.name(), Collections.singleton(webTableName));

        Map<String, Set<String>> timeline2RelatedStreamTables = new HashMap<>();
        timeline2RelatedStreamTables.put(BusinessEntity.Contact.name(), Collections.singleton(ctkTableName));

        Map<String, Set<String>> timeline3RelatedStreamTables = new HashMap<>();
        timeline3RelatedStreamTables.put(BusinessEntity.Account.name(), Sets.newHashSet(oppTableName, webTableName));

        timelineRelatedStreamTables.put(timeLine1.getTimelineId(), timeline1RelatedStreamTables);
        timelineRelatedStreamTables.put(timeLine2.getTimelineId(), timeline2RelatedStreamTables);
        timelineRelatedStreamTables.put(timeLine3.getTimelineId(), timeline3RelatedStreamTables);
        timeLineMap.put(timeLine1.getTimelineId(), timeLine1);
        timeLineMap.put(timeLine2.getTimelineId(), timeLine2);
        timeLineMap.put(timeLine3.getTimelineId(), timeLine3);
        timelineVersionMap.put(timeLine1.getTimelineId(), timeLine1.getTimelineId());
        timelineVersionMap.put(timeLine2.getTimelineId(), timeLine2.getTimelineId());
        timelineVersionMap.put(timeLine3.getTimelineId(), timeLine3.getTimelineId());
    }

    private Object[] testCTKRow(String contactId, String accountId, String activityType, Long rowCount,
                                int nDaysBeforeNow) {
        long time = Instant.ofEpochMilli(now).minus(nDaysBeforeNow, ChronoUnit.DAYS).toEpochMilli();
        String dateStr = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(time));
        Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
        String templateName = String.format("tempalte_%s", dateStr);
        templateToSystemTypeMap.put(templateName, S3ImportSystem.SystemType.Marketo.name());
        List<Object> row = Lists.newArrayList(contactId, accountId, activityType, time, dateStr, datePeriod, rowCount
                , templateName);
        return row.toArray();
    }

    private Object[] testWebRow(long id, int nDaysBeforeNow, String pathPattern) {
        long time = Instant.ofEpochMilli(now).minus(nDaysBeforeNow, ChronoUnit.DAYS).toEpochMilli();
        String dateStr = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(time));
        Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
        String accountId = String.format("a%d", id);
        String templateName = String.format("tempalte_%s", dateStr);
        templateToSystemTypeMap.put(templateName, S3ImportSystem.SystemType.Website.name());
        List<Object> row = Lists.newArrayList(id, accountId, accountId, String.format("Company %d", id),
                pathPattern, time, dateStr, datePeriod, templateName);
        return row.toArray();
    }

    private Object[] testOppRow(long id, String oppId, String stage, int nDaysBeforeNow) {
        long time = Instant.ofEpochMilli(now).minus(nDaysBeforeNow, ChronoUnit.DAYS).toEpochMilli();
        String dateStr = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(time));
        Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
        String accountId = String.format("a%d", id);
        String templateName = String.format("tempalte_%s", dateStr);
        double score = RandomUtils.nextDouble(0.0, 1.0);
        templateToSystemTypeMap.put(templateName, S3ImportSystem.SystemType.Salesforce.name());
        List<Object> row = Lists.newArrayList(id, oppId, accountId, stage, time, dateStr, datePeriod, score,
                templateName);
        return row.toArray();
    }

    private Map<String, DimensionMetadata> webVisitMetadata() {
        Map<String, DimensionMetadata> metadataMap = new HashMap<>();
        metadataMap.put(PathPatternId.name(), ptnMetadata());
        return metadataMap;
    }

    private DimensionMetadata ptnMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        Map<String, Object> content = pathPtnValue("*dnb.com/contents/*", ALL_CTN_PAGE_PTN_NAME);
        Map<String, Object> video = pathPtnValue("*dnb.com/contents/videos/*", VIDEO_CTN_PAGE_PTN_NAME);
        metadata.setDimensionValues(Arrays.asList(content, video));
        metadata.setCardinality(2);
        return metadata;
    }

    private Map<String, Object> pathPtnValue(String pathPattern, String pathPatternName) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(PathPatternId.name(),
                WEBVISIT_DIMENSION_HASH_ID_MAP.get(DimensionGenerator.hashDimensionValue(pathPatternName)));
        valueMap.put(PathPatternName.name(), pathPatternName);
        valueMap.put(PathPattern.name(), pathPattern);
        return valueMap;
    }

    private void preparetimeline1() {
        String timelineName1 = "timelineName1";

        timeLine1 = new TimeLine();
        timeLine1.setName(timelineName1);
        timeLine1.setTimelineId(timelineName1);
        timeLine1.setEntity(BusinessEntity.Account.name());
        timeLine1.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.MarketingActivity));
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));

        timeLine1.setEventMappings(mappingMap);
    }

    private void preparetimeline2() {
        String timelineName = "timelineName2";

        timeLine2 = new TimeLine();
        timeLine2.setName(timelineName);
        timeLine2.setTimelineId(timelineName);
        timeLine2.setEntity(BusinessEntity.Contact.name());
        timeLine2.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.MarketingActivity));
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));

        timeLine2.setEventMappings(mappingMap);
    }

    private void preparetimeline3() {
        String timelineName = "timelineName3";

        timeLine3 = new TimeLine();
        timeLine3.setName(timelineName);
        timeLine3.setTimelineId(timelineName);
        timeLine3.setEntity(BusinessEntity.Account.name());
        timeLine3.setStreamTypes(Arrays.asList(AtlasStream.StreamType.Opportunity, AtlasStream.StreamType.WebVisit));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));
        Map<String, EventFieldExtractor> oppMappings = new HashMap<>(
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.Opportunity));
        // add detail2 to testing casting from long to string
        oppMappings.put(TimeLineStoreUtils.TimelineStandardColumn.Detail2.getColumnName(),
                new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute)
                        .withMappingValue(BuyingScore.name()).build());
        mappingMap.put(AtlasStream.StreamType.Opportunity.name(),
                oppMappings);

        timeLine3.setEventMappings(mappingMap);
    }

    private TimeLineJobConfig baseConfig() {
        TimeLineJobConfig config = new TimeLineJobConfig();
        config.streamTypeWithTableNameMap = streamTypeWithTableNameMap;
        config.timelineRelatedStreamTables = timelineRelatedStreamTables;
        config.contactTableIdx = contactTableIdx;
        config.rawStreamInputIdx = rawStreamInputIdx;
        config.timeLineMap = timeLineMap;
        config.sortKey = "sortKey";
        config.partitionKey = "partitionKey";
        config.timelineVersionMap = timelineVersionMap;
        config.templateToSystemTypeMap = templateToSystemTypeMap;
        config.needRebuild = true;
        config.timelineRelatedMasterTables = new HashMap<>();
        config.tableRoleSuffix = "TEST_ROLE";
        config.tableNameToStreamIdMap = tableNameToStreamIdMap;
        config.dimensionMetadataMap = dimensionMetadataMap;
        return config;
    }

    private Map<String, List<String>> prepareExpectedDetail2Result() {
        Map<String, List<String>> detail2ResultMap = new HashMap<>();
        detail2ResultMap.put("https://dnb.com/contents/audios/1", Collections.singletonList("all content pages"));
        detail2ResultMap.put("https://dnb.com/contents/videos/1", Arrays.asList("all content pages",
                "all video content pages"));
        detail2ResultMap.put("https://dnb.com/contents/videos/2", Arrays.asList("all content pages",
                "all video content pages"));
        detail2ResultMap.put("https://dnb.com/contents/audios/5", Collections.singletonList("all content pages"));
        return detail2ResultMap;
    }
}
