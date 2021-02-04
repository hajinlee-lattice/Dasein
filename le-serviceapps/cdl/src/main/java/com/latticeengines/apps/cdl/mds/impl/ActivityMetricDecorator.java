package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;
import static com.latticeengines.domain.exposed.query.BusinessEntity.WebVisitProfile;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class ActivityMetricDecorator implements Decorator {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricDecorator.class);

    // all activity metric serving entities shares the system attrs
    private final Set<String> systemAttrs = SchemaRepository //
            .getSystemAttributes(WebVisitProfile, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private final String signature;
    private final Tenant tenant;
    private DimensionMetadataService dimensionMetadataService;
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;
    private ActivityStoreService activityStoreService;

    private final ThreadLocal<Boolean> setTenantCtx = new ThreadLocal<>();

    private ConcurrentMap<String, Map<String, DimensionMetadata>> metadataCache = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ActivityMetricsGroup> groupCache = new ConcurrentHashMap<>();
    Map<AtlasStream.StreamType, List<String>> streamTypeNameMap = new HashMap<>();

    ActivityMetricDecorator(String signature, Tenant tenant, //
                            DimensionMetadataService dimensionMetadataService, //
                            ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr, //
                            ActivityStoreService activityStoreService) {
        this.signature = signature;
        this.tenant = tenant;
        this.dimensionMetadataService = dimensionMetadataService;
        this.activityMetricsGroupEntityMgr = activityMetricsGroupEntityMgr;
        this.activityStoreService = activityStoreService;
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        streamTypeNameMap = activityStoreService.getStreamTypeToStreamNamesMap(tenant.getId());
        return metadata.map(this::filter);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        streamTypeNameMap = activityStoreService.getStreamTypeToStreamNamesMap(tenant.getId());
        return metadata.map(this::filter);
    }

    @Override
    public String getName() {
        return "activity-metric-attrs";
    }

    private ColumnMetadata filter(ColumnMetadata cm) {
        switch (cm.getEntity()) {
            case WebVisitProfile:
                cm.setCategory(Category.WEB_VISIT_PROFILE);
                break;
            case Opportunity:
                cm.setCategory(Category.OPPORTUNITY_PROFILE);
                break;
            case AccountMarketingActivity:
                cm.setCategory(Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE);
                break;
            case ContactMarketingActivity:
                cm.setCategory(Category.CONTACT_MARKETING_ACTIVITY_PROFILE);
                break;
            default:
        }
        if (systemAttrs.contains(cm.getAttrName())) {
            return cm;
        }

        cm.enableGroup(Segment);
        cm.disableGroup(Enrichment);
        cm.disableGroup(TalkingPoint);
        cm.disableGroup(CompanyProfile);
        cm.disableGroup(Model);
        cm.setCanSegment(true);
        cm.setCanEnrich(true);
        cm.setCanModel(false);

        try {
            enrichColumnMetadata(cm);
        } catch (Exception e) {
            log.warn("Error while rendering the column " + cm.getAttrName(), e);
        }
        return cm;
    }

    private void enrichColumnMetadata(ColumnMetadata cm) {
        if (!Boolean.TRUE.equals(setTenantCtx.get())) {
            MultiTenantContext.setTenant(tenant);
            setTenantCtx.set(Boolean.TRUE);
        }

        String attrName = cm.getAttrName();
        List<String> tokens;
        try {
            tokens = ActivityMetricsGroupUtils.parseAttrName(attrName);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse metric attribute " + attrName, e);
        }

        String groupId = tokens.get(0);
        if (!groupCache.containsKey(groupId)) {
            ActivityMetricsGroup group = activityMetricsGroupEntityMgr.findByGroupId(groupId);
            if (group != null) {
                groupCache.put(groupId, group);
            }
        }
        ActivityMetricsGroup group = groupCache.get(groupId);


        String[] rollupDimVals = tokens.get(1).split("_");
        String timeRange = tokens.get(2);
        Map<String, Object> params = Collections.emptyMap();
        if (BooleanUtils.isNotTrue(cm.getShouldDeprecate())) {
            // only render for active attributes
            params = getRenderParams(attrName, group, rollupDimVals, timeRange);
            renderTemplates(cm, group, params);
        } else {
            log.debug("Attribute {} is deprecated (shouldDeprecate={}), skip rendering", cm.getAttrName(),
                    cm.getShouldDeprecate());
        }
        renderFundamentalType(cm, group);
        overwriteColumnSelection(cm, group);

        BusinessEntity entity = cm.getEntity();
        AtlasStream.StreamType streamType = group.getStream().getStreamType();
        switch (entity) {
        case WebVisitProfile:
            String pathPtn = ActivityStoreUtils.getDimensionValueAsString(params, InterfaceName.PathPatternId.name(),
                    InterfaceName.PathPattern.name(), tenant);
            ActivityStoreUtils.setColumnMetadataUIProperties(cm, timeRange, pathPtn);
            if (StringUtils.isBlank(pathPtn)) {
                log.warn("Failed to retrieve path pattern for attribute {} in group {} for tenant {}", cm.getAttrName(),
                        group.getGroupId(), tenant.getName());
            }
            break;
        case AccountMarketingActivity:
        case ContactMarketingActivity:
            String activityType = ActivityStoreUtils.getDimensionValueAsString(params,
                    InterfaceName.ActivityTypeId.name(), InterfaceName.ActivityType.name(), tenant);
            ActivityStoreUtils.setColumnMetadataUIProperties(cm, timeRange, activityType);
            if (StringUtils.isBlank(activityType)) {
                log.warn("Failed to retrieve activity type for attribute {} in group {} for tenant {}", cm.getAttrName(),
                        group.getGroupId(), tenant.getName());
            }
            break;
        case Opportunity:
        case CustomIntent:
            // do nothing atm
            break;
        default:
            log.warn("Unrecognized activity metrics entity {} for attribute {}", cm.getEntity(), cm.getAttrName());
        }
        if (entity != WebVisitProfile && streamType != null) { // webVisit stream name doesn't contain system name
            if (streamTypeNameMap.get(streamType).size() > 1) {
                ActivityStoreUtils.appendSystemName(cm, group.getStream().getName());
            }
        }
    }

    private void overwriteColumnSelection(ColumnMetadata cm, ActivityMetricsGroup group) {
        if (MapUtils.isNotEmpty(group.getCsOverwrite())) {
            group.getCsOverwrite().getOrDefault(ActivityMetricsGroup.ColumnSelectionStatus.ENABLE, Collections.emptySet())
                    .forEach(cm::enableGroup);
            group.getCsOverwrite().getOrDefault(ActivityMetricsGroup.ColumnSelectionStatus.DISABLE, Collections.emptySet())
                    .forEach(cm::disableGroup);
        }
    }

    private Map<String, Object> getRenderParams(String attrName, ActivityMetricsGroup group, //
                                                String[] rollupDimVals, String timeRange) {
        String groupId = group.getGroupId();
        String streamId = group.getStream().getStreamId();

        Map<String, Object> params = new HashMap<>();
        String[] rollupDimNames = group.getRollupDimensions().split(",");
        if (rollupDimNames.length != rollupDimVals.length) {
            throw new IllegalArgumentException(
                    String.format("There are %d dimensions in attribute %s, but only %d was defined in group %s", //
                            rollupDimVals.length, attrName, rollupDimNames.length, groupId));
        }

        for (int i = 0; i < rollupDimNames.length; i++) {
            String dimName = rollupDimNames[i];
            String dimVal = rollupDimVals[i];

            if (!metadataCache.containsKey(streamId)) {
                metadataCache.put(streamId, dimensionMetadataService.getMetadataInStream(signature, streamId));
            }
            Map<String, DimensionMetadata> allDimMetadata = metadataCache.get(streamId);
            if (MapUtils.isEmpty(allDimMetadata)) {
                throw new IllegalArgumentException( //
                        String.format("Cannot find dimension metadata for stream %s, in attribute %s", //
                                streamId, attrName));
            }

            Map<String, Object> dimParams = new HashMap<>();
            if (allDimMetadata.containsKey(dimName)) {
                DimensionMetadata dimMetadata = allDimMetadata.get(dimName);
                dimParams.putAll(dimMetadata.getDimensionValues().stream() //
                        .filter(row -> dimVal.equalsIgnoreCase(row.get(dimName).toString())) //
                        .findFirst().orElse(new HashMap<>()));
            }
            if (MapUtils.isEmpty(dimParams)) {
                throw new IllegalArgumentException( //
                        String.format("Cannot find dimension metadata for %s=%s, in attribute %s", //
                                dimName, dimVal, attrName));
            }
            params.put(dimName, dimParams);
        }

        try {
            String timeDesc = ActivityMetricsGroupUtils.timeRangeTmplToDescription(timeRange);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN, timeDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse time range for attribute " + attrName, e);
        }

        try {
            String nextTimeRangePeriodOnly = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 1);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_NEXT_RANGE_PERIOD_ONLY_TOKEN, nextTimeRangePeriodOnly);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_NEXT_RANGE_PERIOD_ONLY_TOKEN, Strings.EMPTY);
        }

        try {
            String timeRangePeriodOnly = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 0);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_PERIOD_ONLY_TOKEN, timeRangePeriodOnly);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_PERIOD_ONLY_TOKEN, Strings.EMPTY);
        }

        try {
            String periodStrategy = ActivityMetricsGroupUtils.getPeriodStrategyFromTimeRange(timeRange);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_PERIOD_STRATEGY_TOKEN, periodStrategy);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_PERIOD_STRATEGY_TOKEN, Strings.EMPTY);
        }

        return params;
    }

    /*
     * fill missing fundamental type for backward compatibility
     */
    private void renderFundamentalType(@NotNull ColumnMetadata cm, @NotNull ActivityMetricsGroup group) {
        /*-
         * when transforming from Attribute to ColumnMetadata, FundamentalType.ALPHA
         * will be set as default if Attribute doesn't have it, so still need to force
         * override if it's ALPHA.
         */
        if ((cm.getFundamentalType() != null && cm.getFundamentalType() != FundamentalType.ALPHA)
                || group.getAggregation() == null) {
            return;
        }
        if (group.getAggregation().getTargetFundamentalType() != null) {
            cm.setFundamentalType(group.getAggregation().getTargetFundamentalType());
        }
    }

    private void renderTemplates(ColumnMetadata cm, ActivityMetricsGroup group, Map<String, Object> params) {
        String attrName = cm.getAttrName();

        String dispNameTmpl = group.getDisplayNameTmpl().getTemplate();
        if (StringUtils.isNotBlank(dispNameTmpl)) {
            try {
                cm.setDisplayName(getTrimmedTemplate(dispNameTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render display name for attribute " + attrName, e);
            }
        }

        String descTmpl = group.getDescriptionTmpl() == null ? null : group.getDescriptionTmpl().getTemplate();
        if (StringUtils.isNotBlank(descTmpl)) {
            try {
                cm.setDescription(getTrimmedTemplate(descTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render description for attribute " + attrName, e);
            }
        }

        String subCatTmpl = group.getSubCategoryTmpl().getTemplate();
        if (StringUtils.isNotBlank(subCatTmpl)) {
            try {
                cm.setSubcategory(getTrimmedTemplate(subCatTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render sub-category for attribute " + attrName, e);
            }
        }
    }

    private String getTrimmedTemplate(String template, Map<String, Object> params) {
        String result = TemplateUtils.renderByMap(template, params);
        if (StringUtils.isNotBlank(result)) {
            result = result.trim();
        }
        return result;
    }
}
