package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.StringTemplateConstants.ACTIVITY_METRICS_GROUP_ATTRNAME;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static org.mockito.ArgumentMatchers.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.mockito.Mockito;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.impl.ActivityRelatedEntityMgrImplTestNGBase;
import com.latticeengines.apps.cdl.mds.ActivityMetricDecoratorFac;
import com.latticeengines.apps.cdl.repository.reader.StringTemplateReaderRepository;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.FilterOptions;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StringTemplate;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;

import reactor.core.publisher.Flux;

public class ActivityMetricDecoratorTestNG extends ActivityRelatedEntityMgrImplTestNGBase {

    private static final String CATALOG_WEBVISIT = "WebVisitPathPatterns";
    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String DIM_PATH_PATTERN_ID = PathPatternId.name();
    private static final String PATTERN_ID = "id123";
    private static final String PATTERN_NAME = "Page 123";
    private static final String PATTERN = "*dnb.com/*";
    private static final String PERIOD = PeriodStrategy.Template.Week.name();
    private static final String GROUPNAME_TOTAL_VISIT = "Total Web Visits";
    private static final String GROUPNAME_TOTAL_VISIT_CW = "Total Web Visits Current Week";
    private static final String DISPLAY_NAME_TMPL = StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME;
    private static final String DESCRIPTION_TMPL = StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION;
    private static final String DESCRIPTION_TMPL_CW = StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION_CW;
    private static final String SUBCATEGORY_TMPL = StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY;
    private static final String JAVA_CLASS_LONG = Long.class.getSimpleName();
    private static final NullMetricsImputation NULL_IMPUTATION = NullMetricsImputation.ZERO;

    private static Map<String, StringTemplate> templateCache = new HashMap<>();

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private ActivityMetricDecoratorFac decoratorFac;

    @Inject
    private StringTemplateReaderRepository stringTemplateReaderRepository;

    private String groupId;
    private String currentWeekGroupId;

    @Override
    protected List<String> getCatalogNames() {
        return Collections.singletonList(CATALOG_WEBVISIT);
    }

    @Override
    protected List<String> getStreamNames() {
        return Collections.singletonList(STREAM_WEBVISIT);
    }


    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        prepareDataFeed();
        prepareCatalog();
        prepareStream();
        prepareDimension();
        prepareMetricGroup();
        String signature = String.valueOf(System.currentTimeMillis());
        mockBatonService();
        mockDataCollectionService(signature);
        mockDimensionMetadataService();
    }


    @Test(groups = "functional")
    private void testDecorator() {
        Decorator decorator = decoratorFac.getDecorator(CustomerSpace.shortenCustomerSpace(mainCustomerSpace));
        List<ColumnMetadata> cols = constructColumns();
        List<ColumnMetadata> rendered = decorator.render(Flux.fromIterable(cols)).collectList().block();
        Assert.assertNotNull(rendered);
        Assert.assertEquals(CollectionUtils.size(rendered), CollectionUtils.size(cols));

        ColumnMetadata cm1 = rendered.get(0);
        Assert.assertEquals(cm1.getCategory(), Category.WEB_VISIT_PROFILE);
        Assert.assertEquals(cm1.getDisplayName(), "8 weeks till today");
        Assert.assertEquals(cm1.getDescription(), "This attribute shows companies with the unique Website Visits counts (organic + lead) within the previous 8 weeks plus this week till today, based on real-time web traffic records. The previous 8 weeks is the 9 weeks period beginning 8 weeks ago as of the last data refresh.");
        Assert.assertEquals(cm1.getSubcategory(), "Page 123");
        Assert.assertEquals(cm1.getSecondarySubCategoryDisplayName(), PATTERN);
        Assert.assertEquals(cm1.getFilterTags(), Arrays.asList("wi_8_w", FilterOptions.Option.ANY_VALUE, "8 weeks"));
        Assert.assertTrue(BooleanUtils.isNotTrue(cm1.isHiddenInCategoryTile()));
        Assert.assertEquals(cm1.getFundamentalType(), FundamentalType.NUMERIC);

        ColumnMetadata cm2 = rendered.get(1);
        Assert.assertEquals(cm2.getCategory(), Category.WEB_VISIT_PROFILE);
        Assert.assertEquals(cm2.getDisplayName(), "1 week till today");
        Assert.assertEquals(cm2.getDescription(), "This attribute shows companies with the unique Website Visits counts (organic + lead) within the previous 1 week plus this week till today, based on real-time web traffic records. The previous 1 week is the 2 weeks period beginning 1 week ago as of the last data refresh.");
        Assert.assertEquals(cm2.getSubcategory(), "Page 123");
        Assert.assertEquals(cm2.getSecondarySubCategoryDisplayName(), PATTERN);
        Assert.assertEquals(cm2.getFilterTags(), Arrays.asList("wi_1_w", FilterOptions.Option.ANY_VALUE, "1 week"));
        Assert.assertTrue(cm2.isHiddenInCategoryTile());
        Assert.assertEquals(cm2.getFundamentalType(), FundamentalType.NUMERIC);

        ColumnMetadata cm3 = rendered.get(2);
        Assert.assertEquals(cm3.getCategory(), Category.WEB_VISIT_PROFILE);
        Assert.assertEquals(cm3.getDisplayName(), "Current week till today");
        Assert.assertEquals(cm3.getDescription(), "This attribute shows companies with the unique Website Visits counts between the beginning of this week till today, based on real-time web traffic records.");
        Assert.assertEquals(cm3.getSubcategory(), "Page 123");
        Assert.assertEquals(cm3.getSecondarySubCategoryDisplayName(), PATTERN);
        Assert.assertEquals(cm3.getFilterTags(), Arrays.asList("wi_0_w", FilterOptions.Option.ANY_VALUE));
        Assert.assertTrue(cm3.isHiddenInCategoryTile());
        Assert.assertEquals(cm3.getFundamentalType(), FundamentalType.NUMERIC);
    }

    private void mockBatonService() {
        BatonService batonService = Mockito.mock(BatonService.class);
        Mockito.when(batonService.isEntityMatchEnabled(any())).thenReturn(true);
        ReflectionTestUtils.setField(decoratorFac, "batonService", batonService);
    }

    private void mockDimensionMetadataService() {
        DimensionMetadataService dimensionMetadataService = Mockito.mock(DimensionMetadataService.class);
        Mockito.when(dimensionMetadataService.getMetadataInStream(any(), any())).thenReturn(getDimensionMetadata());
        ReflectionTestUtils.setField(decoratorFac, "dimensionMetadataService", dimensionMetadataService);
    }

    private void mockDataCollectionService(String signature) {
        DataCollectionService dataCollectionService = Mockito.mock(DataCollectionService.class);
        DataCollection dataCollection = new DataCollection();
        dataCollection.setVersion(DataCollection.Version.Blue);
        DataCollectionStatus status = new DataCollectionStatus();
        status.setDimensionMetadataSignature(signature);
        Mockito.when(dataCollectionService.getDefaultCollection(any())).thenReturn(dataCollection);
        Mockito.when(dataCollectionService.getOrCreateDataCollectionStatus(any(), any())).thenReturn(status);
        ReflectionTestUtils.setField(decoratorFac, "dataCollectionService", dataCollectionService);
    }

    private Map<String, DimensionMetadata> getDimensionMetadata() {
        Map<String, Object> row = new HashMap<>();
        row.put(PathPatternId.name(), PATTERN_ID);
        row.put(PathPatternName.name(), PATTERN_NAME);
        row.put(PathPattern.name(), PATTERN);
        DimensionMetadata metadata = new DimensionMetadata();
        metadata.setDimensionValues(Collections.singletonList(row));
        Map<String, DimensionMetadata> allMetadata = new HashMap<>();
        allMetadata.put(DIM_PATH_PATTERN_ID, metadata);
        return allMetadata;
    }

    private List<ColumnMetadata> constructColumns() {
        Map<String, Object> multiWeeksGroupAttrs = new HashMap<>();
        multiWeeksGroupAttrs.put("GroupId", groupId);
        multiWeeksGroupAttrs.put("RollupDimIds", Collections.singletonList(PATTERN_ID));

        TimeFilter timeFilter = TimeFilter.withinInclude(8, PERIOD);
        multiWeeksGroupAttrs.put("TimeRange", ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter));
        String attr1 = TemplateUtils.renderByMap(ACTIVITY_METRICS_GROUP_ATTRNAME, multiWeeksGroupAttrs).toLowerCase();
        ColumnMetadata cm1 = new ColumnMetadata(attr1, "String");
        cm1.setEntity(BusinessEntity.WebVisitProfile);

        timeFilter = TimeFilter.withinInclude(1, PERIOD);
        multiWeeksGroupAttrs.put("TimeRange", ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter));
        String attr2 = TemplateUtils.renderByMap(ACTIVITY_METRICS_GROUP_ATTRNAME, multiWeeksGroupAttrs).toLowerCase();
        ColumnMetadata cm2 = new ColumnMetadata(attr2, "String");
        cm2.setEntity(BusinessEntity.WebVisitProfile);

        Map<String, Object> currentWeekGroupAttr = new HashMap<>();
        currentWeekGroupAttr.put("GroupId", currentWeekGroupId);
        currentWeekGroupAttr.put("RollupDimIds", Collections.singletonList(PATTERN_ID));
        timeFilter = TimeFilter.withinInclude(0, PERIOD);
        currentWeekGroupAttr.put("TimeRange", ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter));
        String attr3 = TemplateUtils.renderByMap(ACTIVITY_METRICS_GROUP_ATTRNAME, currentWeekGroupAttr).toLowerCase();
        ColumnMetadata cm3 = new ColumnMetadata(attr3, "String");
        cm3.setEntity(BusinessEntity.WebVisitProfile);
        return Arrays.asList(cm1, cm2, cm3);
    }

    private void prepareDimension() {
        StreamDimension dimension = getWebVisitDimension();
        dimensionEntityMgr.create(dimension);
        Assert.assertNotNull(dimension.getPid());
        // make sure stream & dimension can be saved into data-collection status
        DataCollectionStatus status = new DataCollectionStatus();
        status.setTenant(mainTestTenant);
        status.setDataCollection(dataCollection);
        status.setVersion(DataCollection.Version.Blue);
        status.setAccountCount(10L);
        status.setActivityStreamMap(streams);
        dataCollectionStatusEntityMgr.createOrUpdate(status);
    }

    private void prepareMetricGroup() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            ActivityMetricsGroup group = prepareMetricsGroup();
            activityMetricsGroupEntityMgr.createOrUpdate(group);
            return true;
        });
        RetryTemplate retryCurWeek = RetryUtils.getRetryTemplate(3);
        retryCurWeek.execute(ctx -> {
            ActivityMetricsGroup currentWeekGroup = prepareCurrentWeekMetricsGroup();
            activityMetricsGroupEntityMgr.createOrUpdate(currentWeekGroup);
            return true;
        });
    }

    private StreamDimension getWebVisitDimension() {
        StreamDimension dimension = new StreamDimension();
        dimension.setName(DIM_PATH_PATTERN_ID);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_WEBVISIT));
        dimension.setCatalog(catalogs.get(CATALOG_WEBVISIT));
        dimension.addUsages(StreamDimension.Usage.Pivot);

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(PathPatternName.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dimension.setGenerator(generator);

        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
        calculator.setAttribute(WebVisitPageUrl.name());
        calculator.setPatternAttribute(PathPattern.name());
        calculator.setPatternFromCatalog(true);
        dimension.setCalculator(calculator);

        return dimension;
    }

    private ActivityMetricsGroup prepareMetricsGroup() {
        AtlasStream stream = streams.get(STREAM_WEBVISIT);
        ActivityTimeRange activityTimeRange = createTimeRollup();
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setGroupName(GROUPNAME_TOTAL_VISIT);
        groupId = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(GROUPNAME_TOTAL_VISIT);
        group.setGroupId(groupId);
        group.setTenant(mainTestTenant);
        group.setStream(stream);
        group.setEntity(BusinessEntity.Account);
        group.setRollupDimensions(DIM_PATH_PATTERN_ID);
        group.setAggregation(stream.getAttributeDerivers().get(0));
        group.setActivityTimeRange(activityTimeRange);
        group.setDisplayNameTmpl(getTemplate(DISPLAY_NAME_TMPL));
        group.setDescriptionTmpl(getTemplate(DESCRIPTION_TMPL));
        group.setCategory(Category.WEBSITE_PROFILE);
        group.setSubCategoryTmpl(getTemplate(SUBCATEGORY_TMPL));
        group.setJavaClass(JAVA_CLASS_LONG);
        group.setNullImputation(NULL_IMPUTATION);

        return group;
    }

    private ActivityMetricsGroup prepareCurrentWeekMetricsGroup() {
        AtlasStream stream = streams.get(STREAM_WEBVISIT);
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setGroupName(GROUPNAME_TOTAL_VISIT);
        currentWeekGroupId = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(GROUPNAME_TOTAL_VISIT_CW);
        group.setGroupId(currentWeekGroupId);
        group.setTenant(mainTestTenant);
        group.setStream(stream);
        group.setEntity(BusinessEntity.Account);
        group.setRollupDimensions(DIM_PATH_PATTERN_ID);
        group.setAggregation(stream.getAttributeDerivers().get(0));
        group.setActivityTimeRange(ActivityStoreUtils.currentWeekTimeRange());
        group.setDisplayNameTmpl(getTemplate(DISPLAY_NAME_TMPL));
        group.setDescriptionTmpl(getTemplate(DESCRIPTION_TMPL_CW));
        group.setCategory(Category.WEBSITE_PROFILE);
        group.setSubCategoryTmpl(getTemplate(SUBCATEGORY_TMPL));
        group.setJavaClass(JAVA_CLASS_LONG);
        group.setNullImputation(NULL_IMPUTATION);

        return group;
    }

    private ActivityTimeRange createTimeRollup() {
        ActivityTimeRange activityTimeRange = new ActivityTimeRange();
        activityTimeRange.setOperator(ComparisonType.WITHIN);
        activityTimeRange.setParamSet(new HashSet<>(Collections.singletonList(Collections.singletonList(2))));
        activityTimeRange.setPeriods(new HashSet<>(Collections.singletonList(PERIOD)));
        return activityTimeRange;
    }

    private StringTemplate getTemplate(String name) {
        if (!templateCache.containsKey(name)) {
            templateCache.put(name, stringTemplateReaderRepository.findByName(name));
        }
        StringTemplate tmpl = templateCache.get(name);
        if (tmpl == null) {
            throw new IllegalStateException(String.format("Default template %s is not added to database", name));
        }
        return tmpl;
    }
}
