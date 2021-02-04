package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLTemplateName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.InternalId;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class EnrichWebVisitJobTestNG extends SparkJobFunctionalTestNGBase {

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

    private static final List<Pair<String, Class<?>>> WEB_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(EntityId.name(), String.class), //
            Pair.of("LDC_DUNS", String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(WebVisitDate.name(), Long.class), //
            Pair.of(__StreamDate.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(CDLTemplateName.name(), String.class)
    );

    private static final List<Pair<String, Class<?>>> CATALOG_IMPORT_FIELDS = Arrays.asList(
            Pair.of(PathPattern.name(), String.class),
            Pair.of(PathPatternName.name(), String.class)
    );

    private static final List<Pair<String, Class<?>>> WEB_STREAM_RAW_FIELDS = Arrays.asList(
            Pair.of("account_id", String.class),
            Pair.of("visit_date", Long.class),
            Pair.of("user_id", String.class),
            Pair.of("page_url", String.class),
            Pair.of("source_medium", String.class),
            Pair.of("globalult_sales_usd", String.class),
            Pair.of("domult_sales_usd", String.class),
            Pair.of("globalult_employee_total", String.class),
            Pair.of("domult_employee_total", String.class),
            Pair.of("duns_number", String.class),
            Pair.of("domestic_ultimate_duns_number", String.class),
            Pair.of("global_ultimate_duns_number", String.class),
            Pair.of("sic_code", String.class),
            Pair.of("naics_code", String.class),
            Pair.of("industry", String.class),
            Pair.of( "employee_range", String.class),
            Pair.of("revenue_range", String.class),
            Pair.of("is_primary_domain", Boolean.class),
            Pair.of("domain", String.class),
            Pair.of("company_name", String.class),
            Pair.of("country", String.class),
            Pair.of("state", String.class),
            Pair.of("city", String.class),
            Pair.of("site_level", String.class),
            Pair.of("page_groups", String.class)
    );

    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    private Integer matchedWebVisitInputIdx;
    private Integer catalogInputIdx;

    static {
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(ALL_CTN_PAGE_PTN_HASH, ALL_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(VIDEO_CTN_PAGE_PTN_HASH, VIDEO_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_PAID_SRC_HASH, GOOGLE_PAID_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_ORGANIC_SRC_HASH, GOOGLE_ORGANIC_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(FACEBOOK_PAID_SRC_HASH, FACEBOOK_PAID_SRC_ID);
    }

    @Test(groups = "functional")
    public void test() {
        prepareData();

        SparkJobResult result = runSparkJob(EnrichWebVisitJob.class, baseConfig());
        log.info("Result = {}", JsonUtils.serialize(result));
        log.info("ResultString = {}", result.getOutput());
        String outputStr = result.getOutput();
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(this::verify);
    }

    private Boolean verify(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, List<String>> expectedPageGroupsMap = prepareExpectedPageGroupsResult();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            log.info(debugStr(record, getSelectedAttributes().values()));
            Object pageUrl = record.get("page_url");
            if (pageUrl != null) {
                List<String> expectedPageGroups = expectedPageGroupsMap.get(pageUrl.toString());
                String pageGroups =record.get("page_groups").toString();
                List<String> pageGroupArr = Arrays.asList(pageGroups.split("\\|\\|"));
                log.info("pageUrl is {}, pageGroups is {}.", pageUrl.toString(), pageGroups);
                Assert.assertEquals(new HashSet<>(pageGroupArr), new HashSet<>(expectedPageGroups));
            }
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
        matchedWebVisitInputIdx = 0;
        Object[][] catalogTable = new Object[][]{ //
                {"*dnb.com/contents/*", ALL_CTN_PAGE_PTN_NAME}, //
                {"*dnb.com/contents/videos/*", VIDEO_CTN_PAGE_PTN_NAME}
        };
        uploadHdfsDataUnit(catalogTable, CATALOG_IMPORT_FIELDS);
        catalogInputIdx = 1;
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

    private EnrichWebVisitJobConfig baseConfig() {
        EnrichWebVisitJobConfig config = new EnrichWebVisitJobConfig();
        config.selectedAttributes = getSelectedAttributes();
        config.matchedWebVisitInputIdx = matchedWebVisitInputIdx;
        config.catalogInputIdx = catalogInputIdx;
        config.accountIdCol = EntityId.name();
        return config;
    }

    private static Map<String, String> getSelectedAttributes() {
        Map<String, String> selectedAttributes = new HashMap<>();
        selectedAttributes.put("WebVisitDate", "visit_date");
        selectedAttributes.put("UserId", "user_id");
        selectedAttributes.put("WebVisitPageUrl", "page_url");
        selectedAttributes.put("SourceMedium", "source_medium");
        selectedAttributes.put("LE_GlobalUlt_salesUSD", "globalult_sales_usd");
        selectedAttributes.put("LE_DomUlt_SalesUSD", "domult_sales_usd");
        selectedAttributes.put("LE_GlobalULt_EmployeeTotal", "globalult_employee_total");
        selectedAttributes.put("LE_DomUlt_EmployeeTotal", "domult_employee_total");
        selectedAttributes.put("LDC_DUNS", "duns_number");
        selectedAttributes.put("DOMESTIC_ULTIMATE_DUNS_NUMBER", "domestic_ultimate_duns_number");
        selectedAttributes.put("GLOBAL_ULTIMATE_DUNS_NUMBER", "global_ultimate_duns_number");
        selectedAttributes.put("LE_SIC_CODE", "sic_code");
        selectedAttributes.put("LE_Site_NAICS_Code", "naics_code");
        selectedAttributes.put("LE_INDUSTRY", "industry");
        selectedAttributes.put("LE_EMPLOYEE_RANGE", "employee_range");
        selectedAttributes.put("LE_REVENUE_RANGE", "revenue_range");
        selectedAttributes.put("LE_IS_PRIMARY_DOMAIN", "is_primary_domain");
        selectedAttributes.put("LDC_Domain", "domain");
        selectedAttributes.put("LDC_Name", "company_name");
        selectedAttributes.put("LDC_Country", "country");
        selectedAttributes.put("LDC_State", "state");
        selectedAttributes.put("LDC_City", "city");
        selectedAttributes.put("LE_DNB_TYPE", "site_level");
        selectedAttributes.put("UrlCategories", "page_groups");
        selectedAttributes.put("UtmSource", "utm_source");
        selectedAttributes.put("UtmMedium", "utm_medium");
        selectedAttributes.put("UtmCampaign", "utm_campaign");
        selectedAttributes.put("UtmTerm", "utm_term");
        selectedAttributes.put("UtmContent", "utm_content");
        return selectedAttributes;
    }

    private Map<String, List<String>> prepareExpectedPageGroupsResult() {
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
