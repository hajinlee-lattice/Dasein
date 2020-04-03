package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.util.WebVisitUtils.SOURCE_MEDIUM_GROUPNAME;
import static com.latticeengines.domain.exposed.util.WebVisitUtils.TOTAL_VISIT_GROUPNAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.StringTemplateReaderRepository;
import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StringTemplate;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;

@Service("activityMetricsGroupService")
public class ActivityMetricsGroupServiceImpl implements ActivityMetricsGroupService {

    private static final String OPPORTUNITY_STAGE_GROUPNAME = "Opportunity By Stage";
    private static final String MARKETING_TYPE_ACCOUNT_GROUPNAME = "Marketing By ActivityType And AccountID";
    private static final String MARKETING_TYPE_CONTACT_GROUPNAME = "Marketing By ActivityType And ContactID";
    private static final String DIM_NAME_PATH_PATTERN = InterfaceName.PathPatternId.name();
    private static final String DIM_NAME_SOURCEMEDIUM = InterfaceName.SourceMediumId.name();
    private static final String DIM_NAME_STAGE = InterfaceName.StageNameId.name();
    private static final String DIM_NAME_ACTIVITYTYPE = InterfaceName.ActivityTypeId.name();

    private static Map<String, StringTemplate> templateCache = new HashMap<>();

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
    public List<ActivityMetricsGroup> setupDefaultWebVisitProfile(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup totalVisit = setupDefaultTotalVisitGroup(tenant, stream);
        ActivityMetricsGroup sourceMedium = setupDefaultSourceMediumGroup(tenant, stream);
        activityMetricsGroupEntityMgr.create(totalVisit);
        activityMetricsGroupEntityMgr.create(sourceMedium);
        return Arrays.asList(totalVisit, sourceMedium);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> findByStream(String customerSpace, AtlasStream stream) {
        return activityMetricsGroupEntityMgr.findByStream(stream);
    }

    @Override
    @WithCustomerSpace
    public ActivityMetricsGroup setUpDefaultOpportunityProfile(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup stage = setupDefaultStageGroup(tenant, stream);
        activityMetricsGroupEntityMgr.create(stage);
        return stage;
    }

    @Override
    public List<ActivityMetricsGroup> setupDefaultMarketingProfile(String customerSpace, String streamName) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = atlasStreamEntityMgr.findByNameAndTenant(streamName, tenant);
        ActivityMetricsGroup accountActivityType = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Account, MARKETING_TYPE_ACCOUNT_GROUPNAME, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE);
        ActivityMetricsGroup contactActivityType = setupDefaultMarketingTypeGroup(tenant, stream,
                BusinessEntity.Contact, MARKETING_TYPE_CONTACT_GROUPNAME, Category.CONTACT_MARKETING_ACTIVITY_PROFILE);
        activityMetricsGroupEntityMgr.create(accountActivityType);
        activityMetricsGroupEntityMgr.create(contactActivityType);
        return Arrays.asList(accountActivityType, contactActivityType);
    }

    private ActivityMetricsGroup setupDefaultTotalVisitGroup(Tenant tenant, AtlasStream stream) {
        ActivityMetricsGroup totalVisit = new ActivityMetricsGroup();
        totalVisit.setStream(stream);
        totalVisit.setTenant(tenant);
        totalVisit.setGroupId(getGroupId(TOTAL_VISIT_GROUPNAME));
        totalVisit.setGroupName(TOTAL_VISIT_GROUPNAME);
        totalVisit.setJavaClass(Long.class.getSimpleName());
        totalVisit.setEntity(BusinessEntity.Account);
        totalVisit.setActivityTimeRange(ActivityStoreUtils.defaultTimeRange());
        totalVisit.setRollupDimensions(DIM_NAME_PATH_PATTERN);
        totalVisit.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
        totalVisit.setCategory(Category.WEB_VISIT_PROFILE);
        totalVisit.setSubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY));
        totalVisit.setDisplayNameTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME));
        totalVisit.setDescriptionTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION));
        totalVisit.setNullImputation(NullMetricsImputation.ZERO);
        totalVisit.setSecondarySubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY));
        return totalVisit;
    }

    private ActivityMetricsGroup setupDefaultSourceMediumGroup(Tenant tenant, AtlasStream stream) {
        ActivityMetricsGroup sourceMedium = new ActivityMetricsGroup();
        sourceMedium.setStream(stream);
        sourceMedium.setTenant(tenant);
        sourceMedium.setGroupId(getGroupId(SOURCE_MEDIUM_GROUPNAME));
        sourceMedium.setGroupName(SOURCE_MEDIUM_GROUPNAME);
        sourceMedium.setJavaClass(Long.class.getSimpleName());
        sourceMedium.setEntity(BusinessEntity.Account);
        sourceMedium.setActivityTimeRange(ActivityStoreUtils.defaultTimeRange());
        sourceMedium.setRollupDimensions(String.join(",", Arrays.asList(DIM_NAME_SOURCEMEDIUM, DIM_NAME_PATH_PATTERN)));
        sourceMedium.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
        sourceMedium.setCategory(Category.WEB_VISIT_PROFILE);
        sourceMedium.setSubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY));
        sourceMedium.setDisplayNameTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME));
        sourceMedium.setDescriptionTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION));
        sourceMedium.setNullImputation(NullMetricsImputation.ZERO);
        sourceMedium.setSecondarySubCategoryTmpl(getTemplate(StringTemplateConstants.ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY));
        return sourceMedium;
    }

    private ActivityMetricsGroup setupDefaultStageGroup(Tenant tenant, AtlasStream atlasStream) {
        ActivityMetricsGroup stage = new ActivityMetricsGroup();
        stage.setTenant(tenant);
        stage.setStream(atlasStream);
        stage.setGroupId(getGroupId(OPPORTUNITY_STAGE_GROUPNAME));
        stage.setGroupName(OPPORTUNITY_STAGE_GROUPNAME);
        stage.setJavaClass(Long.class.getSimpleName());
        stage.setEntity(BusinessEntity.Account);
        stage.setActivityTimeRange(createActivityTimeRange(ComparisonType.EVER,
                Collections.singleton(PeriodStrategy.Template.Week.name()), null));
        stage.setRollupDimensions(DIM_NAME_STAGE);
        stage.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
        stage.setCategory(Category.OPPORTUNITY_PROFILE);
        stage.setSubCategoryTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_SUBCATEGORY));
        stage.setDisplayNameTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_STAGENAME_DISPLAYNAME));
        stage.setDescriptionTmpl(getTemplate(StringTemplateConstants.OPPORTUNITY_METRICS_GROUP_STAGENAME_DESCRIPTION));
        stage.setNullImputation(NullMetricsImputation.ZERO);
        stage.setReducer(prepareReducer());
        return stage;
    }

    private ActivityMetricsGroup setupDefaultMarketingTypeGroup(Tenant tenant, AtlasStream atlasStream,
                                                                BusinessEntity entity, String groupName,
                                                                Category category) {
        ActivityMetricsGroup marketingType = new ActivityMetricsGroup();
        marketingType.setTenant(tenant);
        marketingType.setStream(atlasStream);
        marketingType.setGroupId(getGroupId(groupName));
        marketingType.setGroupName(groupName);
        marketingType.setJavaClass(Long.class.getSimpleName());
        marketingType.setEntity(entity);
        marketingType.setActivityTimeRange(ActivityStoreUtils.defaultTimeRange());
        marketingType.setRollupDimensions(DIM_NAME_ACTIVITYTYPE);
        marketingType.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.__Row_Count__.name()),
                InterfaceName.__Row_Count__.name(), StreamAttributeDeriver.Calculation.SUM));
        marketingType.setCategory(category);
        marketingType.setSubCategoryTmpl(getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_SUBCATEGORY));
        marketingType.setDisplayNameTmpl(getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_DISPLAYNAME));
        marketingType.setDescriptionTmpl(getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_ACTIVITYTYPE_DESCRIPTION));
        marketingType.setNullImputation(NullMetricsImputation.ZERO);
        marketingType.setSecondarySubCategoryTmpl(getTemplate(StringTemplateConstants.MARKETING_METRICS_GROUP_SECONDARY_SUBCATEGORY));
        return marketingType;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(InterfaceName.OpportunityId.name()));
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
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(sourceAttrs);
        deriver.setTargetAttribute(targetAttr);
        deriver.setCalculation(calculation);
        return deriver;
    }

    private ActivityTimeRange createActivityTimeRange(ComparisonType operator, Set<String> periods,
                                                      Set<List<Integer>> paramSet) {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(operator);
        timeRange.setPeriods(periods);
        timeRange.setParamSet(paramSet);
        return timeRange;
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
