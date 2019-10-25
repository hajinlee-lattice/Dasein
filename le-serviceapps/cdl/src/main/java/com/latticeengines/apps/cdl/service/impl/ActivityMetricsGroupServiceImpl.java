package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StringTemplates;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.security.Tenant;

@Service("activityMetricsGroupService")
public class ActivityMetricsGroupServiceImpl implements ActivityMetricsGroupService {

    private static final String TOTAL_VISIT_GROUPNAME = "Total Web Visits";
    private static final String SOURCE_MEDIUM_GROUPNAME = "Web Visits By Source Medium";
    private static final String DIM_NAME_PATH_PATTERN = "PathPattern";
    private static final String DIM_NAME_SOURCEMEDIUM = "SourceMedium";

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private AtlasStreamEntityMgr atlasStreamEntityMgr;

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

    private ActivityMetricsGroup setupDefaultTotalVisitGroup(Tenant tenant, AtlasStream stream) {
        ActivityMetricsGroup totalVisit = new ActivityMetricsGroup();
        totalVisit.setStream(stream);
        totalVisit.setTenant(tenant);
        totalVisit.setGroupId(getGroupId(TOTAL_VISIT_GROUPNAME));
        totalVisit.setGroupName(TOTAL_VISIT_GROUPNAME);
        totalVisit.setJavaClass(Long.class.getSimpleName());
        totalVisit.setEntity(BusinessEntity.Account);
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(2));
        paramSet.add(Collections.singletonList(4));
        paramSet.add(Collections.singletonList(8));
        paramSet.add(Collections.singletonList(12));
        totalVisit.setActivityTimeRange(createActivityTimeRange(ComparisonType.LAST,
                Collections.singleton(PeriodStrategy.Template.Week.name()), paramSet));
        totalVisit.setRollupDimensions(DIM_NAME_PATH_PATTERN);
        totalVisit.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.InternalId.name()),
                InterfaceName.TotalVisits.name(), StreamAttributeDeriver.Calculation.SUM));
        totalVisit.setCategory(Category.WEB_VISIT_PROFILE);
        totalVisit.setSubCategoryTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_SUBCATEGORY);
        totalVisit.setDisplayNameTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME);
        totalVisit.setDescriptionTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION);
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
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(2));
        paramSet.add(Collections.singletonList(4));
        paramSet.add(Collections.singletonList(8));
        paramSet.add(Collections.singletonList(12));
        sourceMedium.setActivityTimeRange(createActivityTimeRange(ComparisonType.LAST,
                Collections.singleton(PeriodStrategy.Template.Week.name()), paramSet));
        sourceMedium.setRollupDimensions(String.join(",", Arrays.asList(DIM_NAME_SOURCEMEDIUM, DIM_NAME_PATH_PATTERN)));
        sourceMedium.setAggregation(createAttributeDeriver(Collections.singletonList(InterfaceName.InternalId.name()),
                InterfaceName.TotalVisits.name(), StreamAttributeDeriver.Calculation.SUM));
        sourceMedium.setCategory(Category.WEB_VISIT_PROFILE);
        sourceMedium.setSubCategoryTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_SUBCATEGORY);
        sourceMedium.setDisplayNameTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME);
        sourceMedium.setDescriptionTmpl(StringTemplates.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION);
        return sourceMedium;
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
}
