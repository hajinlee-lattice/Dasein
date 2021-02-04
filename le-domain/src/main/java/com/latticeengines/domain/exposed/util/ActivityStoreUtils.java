package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FilterOptions;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * General activity store helpers
 */
public final class ActivityStoreUtils {

    private ActivityStoreUtils() {
    }

    private static final Logger log = LoggerFactory.getLogger(ActivityStoreUtils.class);

    private static final String STREAM_NAME_FORMAT = "%s_%s"; // <sysName>_<entityType>

    // default time range shown in category tile (current value: last 8 weeks)
    public static final TimeFilter UI_DEFAULT_TIME_FILTER = new TimeFilter(ComparisonType.WITHIN,
            PeriodStrategy.Template.Week.name(), Collections.singletonList(8));
    public static final String DEFAULT_TIME_RANGE = ActivityMetricsGroupUtils
            .timeFilterToTimeRangeTmpl(UI_DEFAULT_TIME_FILTER);
    private static final String CURRENT_WEEK_TIME_RANGE = "wi_0_w";

    /**
     * Transform activity store specific pattern into regular expression. This is to
     * support some use cases such as wildcard (*) and can break some regex.
     *
     * @param activityStorePattern
     *            activity store specific pattern (mostly regex)
     * @return transformed regular expression
     */
    public static String modifyPattern(String activityStorePattern) {
        if (StringUtils.isBlank(activityStorePattern)) {
            return activityStorePattern;
        }

        // replace all * (that is not already .*) to .* to support wildcard
        return activityStorePattern.replaceAll("(?<!\\.)\\*", ".*");
    }

    /*-
     * default time range for metrics group
     * - last 2, 4, 8, 12 weeks
     */
    public static ActivityTimeRange defaultTimeRange() {
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(1));
        paramSet.add(Collections.singletonList(2));
        paramSet.add(Collections.singletonList(4));
        paramSet.add(Collections.singletonList(8));
        paramSet.add(Collections.singletonList(12));
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN_INCLUDE);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(paramSet);
        return timeRange;
    }

    public static ActivityTimeRange currentWeekTimeRange() {
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(0));
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN_INCLUDE);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(paramSet);
        return timeRange;
    }

    public static ActivityTimeRange timelessRange() {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.EVER);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(null);
        return timeRange;
    }

    /*-
     * Time filter options.
     * DisplayName: time range display name
     * Value (match against FilterTag in attr): time range
     */
    public static FilterOptions attrFilterOptions() {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setLabel("Timeframe");
        List<FilterOptions.Option> options = new ArrayList<>();
        options.add(FilterOptions.Option.anyAttrOption());
        options.addAll(ActivityMetricsGroupUtils.toTimeFilters(defaultTimeRange()) //
                .stream() //
                .map(ActivityStoreUtils::timeFilterToFilterOption) //
                .collect(Collectors.toList()));
        options.addAll(ActivityMetricsGroupUtils.toTimeFilters(currentWeekTimeRange())
                .stream() //
                .map(ActivityStoreUtils::timeFilterToFilterOption) //
                .collect(Collectors.toList()));
        filterOptions.setOptions(options);
        return filterOptions;
    }

    // FIXME - Temporary solution. Should use attrFilterOptions after all tenants migrated to new groups
    public static FilterOptions genericFilterOptions() {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setLabel("Timeframe");
        List<FilterOptions.Option> options = new ArrayList<>();
        options.add(FilterOptions.Option.anyAttrOption());
        options.addAll(ActivityMetricsGroupUtils.toTimeFilters(currentWeekTimeRange())
                .stream() //
                .map(ActivityStoreUtils::timeFilterToFilterOption) //
                .collect(Collectors.toList()));
        options.addAll(genericTimeFilters());
        filterOptions.setOptions(options);
        return filterOptions;
    }

    private static List<FilterOptions.Option> genericTimeFilters() {
        return ActivityMetricsGroupUtils.toTimeFilters(defaultTimeRange()).stream().map(filter -> {
            FilterOptions.Option option = new FilterOptions.Option();
            String timeRange = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(filter);
            String filterName = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 0);
            option.setValue(filterName);
            option.setDisplayName(filterName);
            return option;
        }).collect(Collectors.toList());
    }

    private static FilterOptions.Option timeFilterToFilterOption(TimeFilter timeFilter) {
        FilterOptions.Option option = new FilterOptions.Option();
        option.setValue(ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter));
        option.setDisplayName(filterOptionDisplayName(timeFilter));
        return option;
    }

    /*-
     * label shown in web visit category tile
     */
    public static String defaultTimeFilterDisplayName() {
        return filterOptionDisplayName(UI_DEFAULT_TIME_FILTER);
    }

    /*-
     * time filter to display name in option drop down
     */
    public static String filterOptionDisplayName(@NotNull TimeFilter filter) {
        switch (filter.getRelation()) {
            // e.g., Last 2 Weeks
            case WITHIN: return String.format("Last %s %ss", filter.getValues().get(0).toString(), filter.getPeriod());
            case WITHIN_INCLUDE: return ActivityMetricsGroupUtils.timeRangeTmplToDescription(ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(filter));
            default: throw new UnsupportedOperationException(String.format("Relation %s is not supported", filter.getRelation()));
        }
    }

    public static void setColumnMetadataUIProperties(@NotNull ColumnMetadata cm, @NotNull String timeRange, String secondaryDisplayName) {
        setFilterTags(cm, timeRange);
        if (!isDefaultPeriodRange(timeRange)) {
            // leave null for not hidden attrs to save some space
            cm.setIsHiddenInCategoryTile(true);
        }
        if (StringUtils.isNotBlank(secondaryDisplayName)) {
            cm.setSecondarySubCategoryDisplayName(secondaryDisplayName);
        }
    }

    public static void setFilterTags(@NotNull ColumnMetadata cm, @NotNull String timeRange) {
        cm.setFilterTags(getFilterTagsFromTimeRange(timeRange));
    }

    public static boolean isDefaultPeriodRange(String timeRange) {
        return timeRange.endsWith("8_w");
    }

    // params: dimensionId -> key -> dimensionValue
    // e.g., PathPatternId -> PathPattern -> "*.google.com/*"
    public static String getDimensionValueAsString(@NotNull Map<String, Object> params, @NotNull String dimCol,
            @NotNull String key, Tenant tenant) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> dimParams = (Map<String, Object>) params.get(dimCol);
            if (MapUtils.isNotEmpty(dimParams)) {
                Object val = dimParams.get(key);
                return val == null ? null : val.toString();
            } else {
                return null;
            }
        } catch (Exception e) {
            String tenantId = tenant == null ? null : tenant.getId();
            log.warn(String.format("Failed to retrieve value with key %s in dimension %s for tenant %s", key, dimCol,
                    tenantId), e);
            return null;
        }
    }

    // FIXME - remove this after all tenants migrated to new groups
    public static List<String> getFilterTagsFromTimeRange(String timeRange) {
        List<String> tags = new ArrayList<>(Arrays.asList(timeRange, FilterOptions.Option.ANY_VALUE));
        if (!CURRENT_WEEK_TIME_RANGE.equals(timeRange)) {
            tags.add(ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 0));
        }
        return tags;
    }

    public static String getStreamName(String systemName, EntityType entityType) {
        return String.format(STREAM_NAME_FORMAT, systemName, entityType);
    }

    public static String getSystemNameFromStreamName(String streamName) {
        int idx = streamName.lastIndexOf('_');
        if (idx <= 0) {
            log.warn("Unable to get system name from stream name {}", streamName);
            return streamName;
        }
        return streamName.substring(0, idx);
    }

    public static void appendSystemName(ColumnMetadata cm, String streamName) {
        cm.setDisplayName(String.format("%s: %s", getSystemNameFromStreamName(streamName), cm.getDisplayName()));
    }
}
