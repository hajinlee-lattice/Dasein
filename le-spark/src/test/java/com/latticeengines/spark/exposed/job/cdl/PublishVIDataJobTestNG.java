package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLTemplateName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.InternalId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LDC_Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StreamDateId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PublishVIDataJobConfiguration;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PublishVIDataJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TimelineJobTestNG.class);

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

    private static final List<String> SELECTED_ATTRIBUTES = ImmutableList.of("AccountId", "WebVisitDate", "UserId",
            "WebVisitPageUrl", "SourceMedium", "LE_GlobalUlt_salesUSD", "LE_DomUlt_SalesUSD",
            "LE_GlobalULt_EmployeeTotal", "LE_DomUlt_EmployeeTotal", "LDC_DUNS", "DOMESTIC_ULTIMATE_DUNS_NUMBER", "GLOBAL_ULTIMATE_DUNS_NUMBER", "LE_SIC_CODE", "LE_Site_NAICS_Code", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE", "LE_IS_PRIMARY_DOMAIN", "LDC_Domain", "LDC_Name", "LDC_Country", "LDC_State", "LDC_City", "LE_DNB_TYPE", "UrlCategories");


    private static final List<Pair<String, Class<?>>> WEB_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(EntityId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(WebVisitDate.name(), Long.class), //
            Pair.of(__StreamDate.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(CDLTemplateName.name(), String.class)
    );

    private static final List<Pair<String, Class<?>>> LATTICEACCOUNT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LDC_Name.name(), String.class), //
            Pair.of("LE_INDUSTRY", String.class), //
            Pair.of("LDC_City", String.class), //
            Pair.of("LDC_Domain", String.class), //
            Pair.of("LDC_DUNS", String.class));

    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    private Map<String, Integer> inputIdx = new HashMap<>();
    private Integer latticeAccountTableIdx;
    private Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap = new HashMap<>();
    private Map<String, String> webVisitTableNameIsMaps = new HashMap<>();

    static {
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(ALL_CTN_PAGE_PTN_HASH, ALL_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(VIDEO_CTN_PAGE_PTN_HASH, VIDEO_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_PAID_SRC_HASH, GOOGLE_PAID_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_ORGANIC_SRC_HASH, GOOGLE_ORGANIC_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(FACEBOOK_PAID_SRC_HASH, FACEBOOK_PAID_SRC_ID);
    }

    @Test(groups = "functional")
    private void test() {
        prepareData();

        SparkJobResult result = runSparkJob(PublishVIDataJob.class, baseConfig());
        log.info("Result = {}", JsonUtils.serialize(result));
        log.info("ResultString = {}", result.getOutput());
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Set> filterParamMaps = JsonUtils.convertMap(rawMap, String.class, Set.class);
        Map<String, Set<String>> verifyFilterParamMaps = verifyResult();
        for (Map.Entry<String, Set> entry : filterParamMaps.entrySet()) {
            Set<String> filterValues = JsonUtils.convertSet(entry.getValue(), String.class);
            log.info("name is {}, filterValue is {}.", entry.getKey(), filterValues);
            Assert.assertEquals(filterValues, verifyFilterParamMaps.get(entry.getKey()));
        }
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(this::verify);
    }

    private Boolean verify(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            log.info(debugStr(record, SELECTED_ATTRIBUTES));
            log.info("record is {}", record);
            counter.incrementAndGet();
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private void prepareData() {
        Object[][] importWeb = new Object[][]{ //
                testWebRow(100L, "01f0iorkcvc39gpj", 1, "https://dnb.com/contents/audios/1"), //
                testWebRow(101L, "034nxyxyfz2uwyw1", 8, "https://dnb.com/contents/videos/1"), //
                testWebRow(102L, "046vxci0yxv0mo8n", 5, "https://dnb.com/contents/videos/2"), //
                testWebRow(103L, "04wis14pzgqzxxd0", 7, "https://dnb.com/contents/audios/5"), //
        };
        String webTableName = uploadHdfsDataUnit(importWeb, WEB_STREAM_IMPORT_FIELDS);
        inputIdx.put(webTableName, 0);
        String streamId = "web_00q1";
        webVisitTableNameIsMaps.put(streamId, webTableName);
        dimensionMetadataMap.put(streamId, webVisitMetadata());

        Object[][] latticeAccountData = new Object[][] { //
                { "01f0iorkcvc39gpj", "Morgan Stanley", "Finance", "San Francisco", "www.morganstanley.com",
                        "039741717" }, //
                { "034nxyxyfz2uwyw1", "Facebook", "Internet", "Menlo Park", "www.fb.com", "160733585" }, //
                { "046vxci0yxv0mo8n", "Costco", "Grocery", "San Meto", "www.costco.com", "114462769" }, //
                { "04wis14pzgqzxxd0", "Cheesecake Factory", "Restaurant", "Mountainview",
                        "www.thecheesecakefactory.com", "082339268" }, //
        };
        String latticeAccountTableName = uploadHdfsDataUnit(latticeAccountData, LATTICEACCOUNT_FIELDS);
        latticeAccountTableIdx = 1;
    }

    private Object[] testWebRow(long id, String accountId, int nDaysBeforeNow, String pathPattern) {
        long time = Instant.ofEpochMilli(now).minus(nDaysBeforeNow, ChronoUnit.DAYS).toEpochMilli();
        String dateStr = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(time));
        Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
        String templateName = String.format("tempalte_%s", dateStr);
        List<Object> row = Lists.newArrayList(id, accountId, accountId, String.format("Company %d", id),
                pathPattern, time, dateStr, datePeriod, templateName);
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

    private PublishVIDataJobConfiguration baseConfig() {
        PublishVIDataJobConfiguration config = new PublishVIDataJobConfiguration();
        config.isTest = true;
        config.targetNum = 1;
        config.webVisitTableNameIsMaps = webVisitTableNameIsMaps;
        config.filterParams = getFilterParams();
        config.selectedAttributes = SELECTED_ATTRIBUTES;
        config.dimensionMetadataMap = dimensionMetadataMap;
        config.inputIdx = inputIdx;
        config.latticeAccountTableIdx = latticeAccountTableIdx;
        return config;
    }

    private Map<String, String> getFilterParams() {
        Map<String, String> filterParams = new HashMap<>();
        filterParams.put("<INDUSTRY_FILTER>", "LE_INDUSTRY");
        filterParams.put("<PAGE_FILTER>", "UrlCategories");
        return filterParams;
    }

    private Map<String, Set<String>> verifyResult() {
        Map<String, Set<String>> filterValueList = new HashMap<>();
        Set<String> valueList = new HashSet<>();
        valueList.add("Finance");
        valueList.add("Grocery");
        valueList.add("Internet");
        valueList.add("Restaurant");
        filterValueList.put("<INDUSTRY_FILTER>", valueList);
        valueList = new HashSet<>();
        valueList.add("all content pages");
        valueList.add("all video content pages");
        filterValueList.put("<PAGE_FILTER>", valueList);
        return filterValueList;
    }
}
