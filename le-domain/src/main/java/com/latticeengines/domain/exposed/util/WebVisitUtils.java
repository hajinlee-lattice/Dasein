package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FilterOptions;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;

/*-
 * Helper for WebVisit activity store related features.
 * TODO remove this when we remove adhoc web visit API
 */
public final class WebVisitUtils {

    public static final String TOTAL_VISIT_GROUPNAME = "Total Web Visits";
    public static final String SOURCE_MEDIUM_GROUPNAME = "Web Visits By Source Medium";

    // default time range shown in category tile (current value: last 8 weeks)
    private static final TimeFilter DEFAULT_TIME_FILTER = new TimeFilter(ComparisonType.WITHIN,
            PeriodStrategy.Template.Week.name(), Collections.singletonList(8));
    private static final String DEFAULT_TIME_RANGE = ActivityMetricsGroupUtils
            .timeFilterToTimeRangeTmpl(DEFAULT_TIME_FILTER);

    protected WebVisitUtils() {
        throw new UnsupportedOperationException();
    }

    /*
     * all dimensions for web visit
     */
    public static List<StreamDimension> newWebVisitDimensions(@NotNull AtlasStream stream, Catalog pathPtnCatalog,
            Catalog srcMediumCatalog) {
        StreamDimension pathPtnDim = pathPtnDimension(stream, pathPtnCatalog);
        StreamDimension sourceMediumDim = sourceMediumDimension(stream, srcMediumCatalog);
        StreamDimension userIdDim = userIdDimension(stream);
        return Arrays.asList(pathPtnDim, sourceMediumDim, userIdDim);
    }

    /*
     * Source medium profile
     */
    public static StreamDimension sourceMediumDimension(@NotNull AtlasStream stream, Catalog srcMediumCatalog) {
        StreamDimension dim = new StreamDimension();
        dim.setName(InterfaceName.SourceMediumId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.addUsages(StreamDimension.Usage.Pivot);
        dim.setCatalog(srcMediumCatalog);

        // hash source medium
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.SourceMedium.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setName(InterfaceName.SourceMedium.name());
        calculator.setAttribute(InterfaceName.SourceMedium.name());
        dim.setCalculator(calculator);
        return dim;
    }

    /*
     * Calculate # of unique user visits
     */
    public static StreamDimension userIdDimension(@NotNull AtlasStream stream) {
        StreamDimension dim = new StreamDimension();
        dim.setName(InterfaceName.UserId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.addUsages(StreamDimension.Usage.Dedup);

        // use the original value
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.UserId.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.ENUM);
        dim.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setName(InterfaceName.UserId.name());
        calculator.setAttribute(InterfaceName.UserId.name());
        dim.setCalculator(calculator);
        return dim;
    }

    /*-
     * PathPatternId dimension for web visit
     */
    public static StreamDimension pathPtnDimension(@NotNull AtlasStream stream, Catalog pathPtnCatalog) {
        StreamDimension dim = new StreamDimension();
        dim.setName(InterfaceName.PathPatternId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.setCatalog(pathPtnCatalog);
        dim.addUsages(StreamDimension.Usage.Pivot);

        // standardize and hash ptn name for dimension
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.PathPatternName.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);
        // use url attr in stream to determine whether it matches catalog pattern
        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
        calculator.setName(InterfaceName.WebVisitPageUrl.name());
        calculator.setAttribute(InterfaceName.WebVisitPageUrl.name());
        calculator.setPatternAttribute(InterfaceName.PathPattern.name());
        calculator.setPatternFromCatalog(true);
        dim.setCalculator(calculator);
        return dim;
    }

    /*-
     * Stream object for web visit data
     *
     * Period: Week
     * Retention: 1yr
     * Entity: Account
     * Rollup: Unique account visit, All account visit
     */
    public static AtlasStream newWebVisitStream(@NotNull Tenant tenant, @NotNull DataFeedTask dataFeedTask) {
        AtlasStream stream = new AtlasStream();
        stream.setName(EntityType.WebVisit.name());
        stream.setTenant(tenant);
        stream.setDataFeedTask(dataFeedTask);
        stream.setMatchEntities(Collections.singletonList(BusinessEntity.Account.name()));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        stream.setDateAttribute(InterfaceName.WebVisitDate.name());
        stream.setPeriods(Collections.singletonList(PeriodStrategy.Template.Week.name()));
        stream.setRetentionDays(365);
        return stream;
    }

    /*-
     * Only show total visit attrs that belongs to default time range
     */
    public static boolean shouldHideInCategoryTile(@NotNull ColumnMetadata cm, @NotNull ActivityMetricsGroup group,
            @NotNull String timeRange) {
        return !TOTAL_VISIT_GROUPNAME.equals(group.getGroupName()) || !DEFAULT_TIME_RANGE.equals(timeRange);
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
                .map(timeFilter -> {
                    FilterOptions.Option option = new FilterOptions.Option();
                    option.setValue(ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter));
                    option.setDisplayName(filterOptionDisplayName(timeFilter));
                    return option;
                }) //
                .collect(Collectors.toList()));
        filterOptions.setOptions(options);
        return filterOptions;
    }

    /*-
     * time filter to display name in option drop down
     *
     * TODO, move to ActivityMetricsGroupUtils and use StringTemplate
     *       if we need to support filtering for more than WebVisit
     */
    public static String filterOptionDisplayName(@NotNull TimeFilter filter) {
        if (filter.getRelation() != ComparisonType.WITHIN) {
            String msg = String.format("Relation %s is not supported", filter.getRelation());
            throw new UnsupportedOperationException(msg);
        }

        // e.g., Last 2 Weeks
        return String.format("Last %s %ss", filter.getValues().get(0).toString(), filter.getPeriod());
    }

    /*-
     * label shown in web visit category tile
     */
    public static String defaultTimeFilterDisplayName() {
        return filterOptionDisplayName(DEFAULT_TIME_FILTER);
    }

    /*-
     * default time range for metrics group
     * - last 2, 4, 8, 12 weeks
     */
    public static ActivityTimeRange defaultTimeRange() {
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(2));
        paramSet.add(Collections.singletonList(4));
        paramSet.add(Collections.singletonList(8));
        paramSet.add(Collections.singletonList(12));
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(paramSet);
        return timeRange;
    }
}
