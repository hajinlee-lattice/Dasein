package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.util.ActivityStoreUtils.DEFAULT_TIME_RANGE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
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
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;

/*-
 * Helper for WebVisit activity store related features.
 * TODO remove this when we remove adhoc web visit API
 */
public final class WebVisitUtils {

    private static final Logger log = LoggerFactory.getLogger(WebVisitUtils.class);

    public static final String TOTAL_VISIT_GROUPNAME = "Total Web Visits";
    public static final String SOURCE_MEDIUM_GROUPNAME = "Web Visits By Source Medium";

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
        stream.setStreamType(AtlasStream.StreamType.WebVisit);
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

    public static void setColumnMetadataUIProperties(@NotNull ColumnMetadata cm, @NotNull ActivityMetricsGroup group, @NotNull String timeRange, @NotNull Map<String, Object> params) {
        // any tag for filtering all attrs
        cm.setFilterTags(Arrays.asList(timeRange, FilterOptions.Option.ANY_VALUE));
        if (shouldHideInCategoryTile(cm, group, timeRange)) {
            // leave null for not hidden attrs to save some space
            cm.setIsHiddenInCategoryTile(true);
        }

        String pathPtn = ActivityStoreUtils.getDimensionValueAsString(params, InterfaceName.PathPatternId.name(),
                InterfaceName.PathPattern.name(), group.getTenant());
        if (StringUtils.isNotBlank(pathPtn)) {
            cm.setSecondarySubCategoryDisplayName(pathPtn);
        } else {
            String tenantId = group.getTenant() == null ? null : group.getTenant().getId();
            log.warn("Failed to retrieve path pattern for attribute {} in group {} for tenant {}", cm.getAttrName(),
                    group.getGroupId(), tenantId);
        }
    }
}
