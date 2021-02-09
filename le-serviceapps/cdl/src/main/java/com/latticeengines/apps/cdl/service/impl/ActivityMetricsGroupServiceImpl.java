package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.BUYING_STAGE_THRESHOLD;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_BUYING;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_RESEARCHING;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.StringTemplateReaderRepository;
import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.CategorizeDoubleConfig;
import com.latticeengines.domain.exposed.cdl.activity.CreateActivityMetricsGroupRequest;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StringTemplate;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;
import com.latticeengines.domain.exposed.util.WebVisitUtils;

@Service("activityMetricsGroupService")
public class ActivityMetricsGroupServiceImpl implements ActivityMetricsGroupService {

    public static final String TOTAL_VISIT_GROUPNAME = WebVisitUtils.TOTAL_VISIT_GROUPNAME;
    public static final String SOURCE_MEDIUM_GROUPNAME = WebVisitUtils.SOURCE_MEDIUM_GROUPNAME;
    private static final String OPPORTUNITY_STAGE_GROUPNAME = "Opportunity By Stage";
    private static final String MARKETING_TYPE_ACCOUNT_GROUPNAME = "Account Marketing By ActivityType";
    private static final String MARKETING_TYPE_CONTACT_GROUPNAME = "Contact Marketing By ActivityType";
    private static final String INTENTDATA_INTENT_GROUPAME = "Has Intent";
    private static final String INTENTDATA_MODEL_GROUPNAME = "Has Intent By TimeRange";
    private static final String BUYING_SCORE_GROUPNAME = "Buying Stage";
    private static final String DIM_NAME_PATH_PATTERN = InterfaceName.PathPatternId.name();
    private static final String DIM_NAME_SOURCEMEDIUM = InterfaceName.SourceMediumId.name();
    private static final String DIM_NAME_STAGE = InterfaceName.StageNameId.name();
    private static final String DIM_NAME_ACTIVITYTYPE = InterfaceName.ActivityTypeId.name();
    private static final String DIM_NAME_DNBINTENT = InterfaceName.ModelNameId.name();

    private static final Map<String, StringTemplate> templateCache = new HashMap<>();

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private AtlasStreamEntityMgr atlasStreamEntityMgr;

    @Inject
    private StringTemplateReaderRepository stringTemplateReaderRepository;

    @Override
    @WithCustomerSpace
    public ActivityMetricsGroup findByPid(String customerSpace, Long pid) {
        return activityMetricsGroupEntityMgr.findByPid(pid);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> setupDefaultWebVisitGroups(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup totalVisit = setupWebVisitGroup(tenant, stream, ActivityStoreUtils.defaultTimeRange());
        ActivityMetricsGroup totalVisitCurWeek = setupWebVisitGroup(tenant, stream,
                ActivityStoreUtils.currentWeekTimeRange());
        ActivityMetricsGroup sourceMedium = setupSourceMediumGroup(tenant, stream,
                ActivityStoreUtils.defaultTimeRange());
        ActivityMetricsGroup sourceMediumCurWeek = setupSourceMediumGroup(tenant, stream,
                ActivityStoreUtils.currentWeekTimeRange());
        return Arrays.asList(totalVisit, totalVisitCurWeek, sourceMedium, sourceMediumCurWeek);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> findByStream(String customerSpace, AtlasStream stream) {
        return activityMetricsGroupEntityMgr.findByStream(stream);
    }

    @Override
    @WithCustomerSpace
    public ActivityMetricsGroup setUpDefaultOpportunityGroup(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        return setupDefaultOpportunityGroup(tenant, stream);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> setupDefaultMarketingGroups(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup accountActivityType = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Account, MARKETING_TYPE_ACCOUNT_GROUPNAME, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE,
                ActivityStoreUtils.defaultTimeRange());
        ActivityMetricsGroup accountActivityTypeCurWeek = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Account, MARKETING_TYPE_ACCOUNT_GROUPNAME, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE,
                ActivityStoreUtils.currentWeekTimeRange());
        ActivityMetricsGroup contactActivityType = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Contact, MARKETING_TYPE_CONTACT_GROUPNAME, Category.CONTACT_MARKETING_ACTIVITY_PROFILE,
                ActivityStoreUtils.defaultTimeRange());
        ActivityMetricsGroup contactActivityTypeCurWeek = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Contact, MARKETING_TYPE_CONTACT_GROUPNAME, Category.CONTACT_MARKETING_ACTIVITY_PROFILE,
                ActivityStoreUtils.currentWeekTimeRange());

        return Arrays.asList(accountActivityType, accountActivityTypeCurWeek, contactActivityType,
                contactActivityTypeCurWeek);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> setupDefaultDnbIntentGroups(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup hasIntentGroup = setupHasIntentGroup(tenant, stream);
        ActivityMetricsGroup intentByTimeRangeGroup = setupHasIntentByTimeRangeGroup(tenant, stream,
                ActivityStoreUtils.defaultTimeRange());
        ActivityMetricsGroup last1WeekGroup = setupHasIntentByTimeRangeLegacyGroups(tenant, stream);
        ActivityMetricsGroup intentByTimeRangeCurWeekGroup = setupHasIntentByTimeRangeGroup(tenant, stream,
                ActivityStoreUtils.currentWeekTimeRange());
        ActivityMetricsGroup buyingScoreGroup = setupBuyingStageGroup(tenant, stream);

        return Arrays.asList(hasIntentGroup, intentByTimeRangeGroup, intentByTimeRangeCurWeekGroup, buyingScoreGroup);
    }

    @Override
    @WithCustomerSpace
    public boolean createCustomizedGroup(String customerSpace, CreateActivityMetricsGroupRequest request) {
        String streamName = request.streamName;
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        assert stream != null;
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup group = new ActivityMetricsGroup();
            group.setStream(stream);
            group.setTenant(tenant);
            group.setGroupName(request.groupName);
            group.setGroupId(getGroupId(request.groupName));
            group.setJavaClass(request.javaClass);
            group.setEntity(request.entity);
            group.setActivityTimeRange(request.timeRange);
            group.setRollupDimensions(String.join(",", request.rollupDimensions));
            group.setAggregation(request.aggregation);
            group.setCategory(request.category);
            group.setSubCategoryTmpl(getTemplate(request.subCategoryTmpl));
            group.setDisplayNameTmpl(getTemplate(request.displayNameTmpl));
            group.setDescriptionTmpl(StringUtils.isNotBlank(request.descriptionTmpl) ? getTemplate(request.descriptionTmpl) : null);
            group.setSecondarySubCategoryTmpl(StringUtils.isNotBlank(request.secSubCategoryTmpl) ? getTemplate(request.secSubCategoryTmpl): null);
            group.setNullImputation(request.nullImputation);
            group.setReducer(request.reducer);
            activityMetricsGroupEntityMgr.create(group);
            return true;
        });
    }

    private ActivityMetricsGroup setupWebVisitGroup(Tenant tenant, AtlasStream stream, ActivityTimeRange timeRange) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup totalVisit = new ActivityMetricsGroup();
            totalVisit.setStream(stream);
            totalVisit.setTenant(tenant);
            totalVisit.setGroupId(getGroupId(TOTAL_VISIT_GROUPNAME));
            totalVisit.setGroupName(TOTAL_VISIT_GROUPNAME);
            totalVisit.setJavaClass(Long.class.getSimpleName());
            totalVisit.setEntity(BusinessEntity.Account);
            totalVisit.setActivityTimeRange(timeRange);
            totalVisit.setRollupDimensions(DIM_NAME_PATH_PATTERN);
            totalVisit.setAggregation(
                    createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                            InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
            totalVisit.setCategory(Category.WEB_VISIT_PROFILE);
            totalVisit.setSubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY));
            totalVisit.setDisplayNameTmpl(
                    getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME));
            totalVisit.setDescriptionTmpl(getTemplate(isCurrentWeekTimeRange(timeRange)
                    ? StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION_CW
                    : StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION));
            totalVisit.setNullImputation(NullMetricsImputation.ZERO);
            totalVisit.setSecondarySubCategoryTmpl(
                    getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY));
            activityMetricsGroupEntityMgr.create(totalVisit);
            return totalVisit;
        });
    }

    private ActivityMetricsGroup setupSourceMediumGroup(Tenant tenant, AtlasStream stream,
            ActivityTimeRange timeRange) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup sourceMedium = new ActivityMetricsGroup();
            sourceMedium.setStream(stream);
            sourceMedium.setTenant(tenant);
            sourceMedium.setGroupId(getGroupId(SOURCE_MEDIUM_GROUPNAME));
            sourceMedium.setGroupName(SOURCE_MEDIUM_GROUPNAME);
            sourceMedium.setJavaClass(Long.class.getSimpleName());
            sourceMedium.setEntity(BusinessEntity.Account);
            sourceMedium.setActivityTimeRange(timeRange);
            sourceMedium
                    .setRollupDimensions(String.join(",", Arrays.asList(DIM_NAME_SOURCEMEDIUM, DIM_NAME_PATH_PATTERN)));
            sourceMedium.setAggregation(
                    createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                            InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
            sourceMedium.setCategory(Category.WEB_VISIT_PROFILE);
            sourceMedium.setSubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY));
            sourceMedium.setDisplayNameTmpl(
                    getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUMNAME_DISPLAYNAME));
            sourceMedium.setDescriptionTmpl(getTemplate(isCurrentWeekTimeRange(timeRange)
                    ? StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION_CW
                    : StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION));
            sourceMedium.setNullImputation(NullMetricsImputation.ZERO);
            sourceMedium.setSecondarySubCategoryTmpl(
                    getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY));
            activityMetricsGroupEntityMgr.create(sourceMedium);
            return sourceMedium;
        });
    }

    private ActivityMetricsGroup setupDefaultOpportunityGroup(Tenant tenant, AtlasStream atlasStream) {
        ActivityMetricsGroup stage = new ActivityMetricsGroup();
        stage.setTenant(tenant);
        stage.setStream(atlasStream);
        stage.setGroupId(getGroupId(OPPORTUNITY_STAGE_GROUPNAME));
        stage.setGroupName(OPPORTUNITY_STAGE_GROUPNAME);
        stage.setJavaClass(Long.class.getSimpleName());
        stage.setEntity(BusinessEntity.Account);
        stage.setActivityTimeRange(ActivityStoreUtils.timelessRange());
        stage.setRollupDimensions(DIM_NAME_STAGE);
        stage.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
        stage.setCategory(Category.OPPORTUNITY_PROFILE);
        stage.setSubCategoryTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_SUBCATEGORY));
        stage.setDisplayNameTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_NAME_DISPLAYNAME));
        stage.setDescriptionTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_STAGENAME_DESCRIPTION));
        stage.setNullImputation(NullMetricsImputation.ZERO);
        stage.setReducer(prepareReducer());
        activityMetricsGroupEntityMgr.create(stage);
        return stage;
    }

    private ActivityMetricsGroup setupDefaultMarketingTypeGroup(Tenant tenant, AtlasStream atlasStream,
            BusinessEntity entity, String groupName, Category category, ActivityTimeRange timeRange) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup marketingType = new ActivityMetricsGroup();
            marketingType.setTenant(tenant);
            marketingType.setStream(atlasStream);
            marketingType.setGroupId(getGroupId(groupName));
            marketingType.setGroupName(groupName);
            marketingType.setJavaClass(Long.class.getSimpleName());
            marketingType.setEntity(entity);
            marketingType.setActivityTimeRange(timeRange);
            marketingType.setRollupDimensions(DIM_NAME_ACTIVITYTYPE);
            marketingType.setAggregation(
                    createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                            InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
            marketingType.setCategory(category);
            marketingType.setSubCategoryTmpl(
                    getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_SUBCATEGORY));
            marketingType.setDisplayNameTmpl(
                    getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_DISPLAYNAME));
            marketingType.setDescriptionTmpl(getTemplate(isCurrentWeekTimeRange(timeRange)
                    ? StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_DESCRIPTION_CW
                    : StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_DESCRIPTION));
            marketingType.setNullImputation(NullMetricsImputation.ZERO);
            marketingType.setSecondarySubCategoryTmpl(
                    getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_SECONDARY_SUBCATEGORY));
            activityMetricsGroupEntityMgr.create(marketingType);
            return marketingType;
        });
    }

    private ActivityMetricsGroup setupHasIntentGroup(Tenant tenant, AtlasStream atlasStream) {
        ActivityMetricsGroup intentGroup = new ActivityMetricsGroup();
        intentGroup.setTenant(tenant);
        intentGroup.setStream(atlasStream);
        intentGroup.setGroupId(getGroupId(INTENTDATA_INTENT_GROUPAME));
        intentGroup.setGroupName(INTENTDATA_INTENT_GROUPAME);
        intentGroup.setJavaClass(Boolean.class.getSimpleName());
        intentGroup.setEntity(BusinessEntity.Account);
        intentGroup.setActivityTimeRange(ActivityStoreUtils.timelessRange());
        intentGroup.setRollupDimensions(DIM_NAME_DNBINTENT);
        intentGroup.setAggregation(createAttributeDeriver(Collections.emptyList(), InterfaceName.HasIntent.name(),
                StreamAttributeDeriver.Calculation.TRUE, FundamentalType.BOOLEAN));
        intentGroup.setCategory(Category.DNBINTENTDATA_PROFILE);
        intentGroup.setDisplayNameTmpl(
                getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_INTENT_DISPLAYNAME));
        intentGroup.setDescriptionTmpl(
                getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_INTENT_DESCRIPTION));
        intentGroup.setSubCategoryTmpl(
                getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_INTENT_SUBCATEGORY));
        intentGroup.setNullImputation(NullMetricsImputation.FALSE);
        intentGroup.setUseLatestVersion(true);
        activityMetricsGroupEntityMgr.create(intentGroup);
        return intentGroup;
    }

    private ActivityMetricsGroup setupHasIntentByTimeRangeGroup(Tenant tenant, AtlasStream atlasStream,
            ActivityTimeRange timeRange) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup modelGroup = new ActivityMetricsGroup();
            modelGroup.setTenant(tenant);
            modelGroup.setStream(atlasStream);
            modelGroup.setGroupId(getGroupId(INTENTDATA_MODEL_GROUPNAME));
            modelGroup.setGroupName(INTENTDATA_MODEL_GROUPNAME);
            modelGroup.setJavaClass(Boolean.class.getSimpleName());
            modelGroup.setEntity(BusinessEntity.Account);
            modelGroup.setActivityTimeRange(timeRange);
            modelGroup.setRollupDimensions(DIM_NAME_DNBINTENT);
            modelGroup.setAggregation(createAttributeDeriver(Collections.emptyList(), InterfaceName.HasIntent.name(),
                    StreamAttributeDeriver.Calculation.TRUE, FundamentalType.BOOLEAN));
            modelGroup.setCategory(Category.DNBINTENTDATA_PROFILE);
            modelGroup.setDisplayNameTmpl(
                    getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_DISPLAYNAME));
            modelGroup.setDescriptionTmpl(getTemplate(isCurrentWeekTimeRange(timeRange)
                    ? StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_CURRENT_WEEK_DESCRIPTION
                    : StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_DESCRIPTION));
            modelGroup.setSubCategoryTmpl(
                    getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_SUBCATEGORY));
            modelGroup.setNullImputation(NullMetricsImputation.FALSE);
            modelGroup.setUseLatestVersion(false);
            activityMetricsGroupEntityMgr.create(modelGroup);
            return modelGroup;
        });
    }

    private ActivityMetricsGroup setupHasIntentByTimeRangeLegacyGroups(Tenant tenant, AtlasStream atlasStream) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            ActivityMetricsGroup last1WeekGroup = new ActivityMetricsGroup();
            last1WeekGroup.setTenant(tenant);
            last1WeekGroup.setStream(atlasStream);
            last1WeekGroup.setGroupId(getGroupId(INTENTDATA_MODEL_GROUPNAME));
            last1WeekGroup.setGroupName(INTENTDATA_MODEL_GROUPNAME);
            last1WeekGroup.setJavaClass(Boolean.class.getSimpleName());
            last1WeekGroup.setEntity(BusinessEntity.Account);
            last1WeekGroup.setActivityTimeRange(getLast1WeekTimeRange());
            last1WeekGroup.setRollupDimensions(DIM_NAME_DNBINTENT);
            last1WeekGroup.setAggregation(createAttributeDeriver(Collections.emptyList(), InterfaceName.HasIntent.name(),
                    StreamAttributeDeriver.Calculation.TRUE, FundamentalType.BOOLEAN));
            last1WeekGroup.setCategory(Category.DNBINTENTDATA_PROFILE);
            last1WeekGroup.setDisplayNameTmpl(
                    getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_DISPLAYNAME));
            last1WeekGroup.setDescriptionTmpl(getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_DESCRIPTION));
            last1WeekGroup.setSubCategoryTmpl(
                    getTemplate(StringTemplateConstants.DNBINTENTDATA_METRICS_GROUP_MODEL_SUBCATEGORY));
            last1WeekGroup.setNullImputation(NullMetricsImputation.FALSE);
            last1WeekGroup.setUseLatestVersion(false);
            last1WeekGroup.disable(Collections.singleton(ColumnSelection.Predefined.Segment));
            activityMetricsGroupEntityMgr.create(last1WeekGroup);
            return last1WeekGroup;
        });
    }

    private ActivityTimeRange getLast1WeekTimeRange() {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(Collections.singleton(Collections.singletonList(1)));
        return timeRange;
    }

    private ActivityMetricsGroup setupBuyingStageGroup(Tenant tenant, AtlasStream atlasStream) {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setTenant(tenant);
        group.setStream(atlasStream);
        group.setGroupId(getGroupId(BUYING_SCORE_GROUPNAME));
        group.setGroupName(BUYING_SCORE_GROUPNAME);
        group.setJavaClass(String.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(ActivityStoreUtils.timelessRange());
        group.setRollupDimensions(DIM_NAME_DNBINTENT);
        group.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.BuyingScore.name()),
                InterfaceName.BuyingScore.name(), StreamAttributeDeriver.Calculation.MAX));
        group.setCategory(Category.DNBINTENTDATA_PROFILE);
        group.setDisplayNameTmpl(getTemplate(StringTemplateConstants.BUYINGSCORE_METRICS_GROUP_DISPLAYNAME));
        group.setDescriptionTmpl(getTemplate(StringTemplateConstants.BUYINGSCORE_METRICS_GROUP_DESCRIPTION));
        group.setSubCategoryTmpl(getTemplate(StringTemplateConstants.BUYINGSCORE_METRICS_GROUP_SUBCATEGORY));
        group.setNullImputation(NullMetricsImputation.NULL);
        group.setUseLatestVersion(false);
        group.setCategorizeValConfig(constructCategorizeDoubleConfig());
        group.setReducer(prepareBuyingStageGroupReducer());
        activityMetricsGroupEntityMgr.create(group);
        return group;
    }

    private CategorizeDoubleConfig constructCategorizeDoubleConfig() {
        CategorizeDoubleConfig config = new CategorizeDoubleConfig();
        config.getCategories().put(STAGE_BUYING,
                Collections.singletonMap(CategorizeDoubleConfig.Comparator.GE, BUYING_STAGE_THRESHOLD));
        config.getCategories().put(STAGE_RESEARCHING,
                Collections.singletonMap(CategorizeDoubleConfig.Comparator.LT, BUYING_STAGE_THRESHOLD));
        return config;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(InterfaceName.OpportunityId.name()));
        reducer.setArguments(Collections.singletonList(InterfaceName.PeriodId.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private ActivityRowReducer prepareBuyingStageGroupReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ModelNameId.name()));
        reducer.setArguments(Collections.singletonList(InterfaceName.PeriodId.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private String getGroupId(String groupName) {
        String base = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        return activityMetricsGroupEntityMgr.getNextAvailableGroupId(base);
    }

    private StreamAttributeDeriver createAttributeDeriver(List<String> sourceAttrs, String targetAttr,
            StreamAttributeDeriver.Calculation calculation) {
        return createAttributeDeriver(sourceAttrs, targetAttr, calculation, FundamentalType.NUMERIC);
    }

    private StreamAttributeDeriver createAttributeDeriver(List<String> sourceAttrs, String targetAttr,
            StreamAttributeDeriver.Calculation calculation, FundamentalType type) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(sourceAttrs);
        deriver.setTargetAttribute(targetAttr);
        deriver.setCalculation(calculation);
        deriver.setTargetFundamentalType(type);
        return deriver;
    }

    private boolean isCurrentWeekTimeRange(ActivityTimeRange timeRange) {
        return timeRange.getParamSet().iterator().next().get(0) == 0;
    }

    private StringTemplate getTemplate(String name) {
        StringTemplate tmpl = templateCache.get(name);
        if (tmpl == null) {
            tmpl = stringTemplateReaderRepository.findByName(name);
            if (tmpl == null) {
                throw new IllegalStateException(String.format("Default template %s is not added to database", name));
            }
            templateCache.put(name, tmpl);
        }
        return tmpl;
    }
}
