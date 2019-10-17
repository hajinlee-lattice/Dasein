package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;

/*-
 * Helper for WebVisit activity store related features.
 * TODO remove this when we remove adhoc web visit API
 */
public class WebVisitUtils {

    /*
     * all dimensions for web visit
     */
    public static List<StreamDimension> newWebVisitDimensions(@NotNull AtlasStream stream, Catalog pathPtnCatalog) {
        StreamDimension pathPtnDim = pathPtnDimension(stream, pathPtnCatalog);
        StreamDimension sourceMediumDim = sourceMediumDimension(stream);
        StreamDimension userIdDim = userIdDimension(stream);
        return Arrays.asList(pathPtnDim, sourceMediumDim, userIdDim);
    }

    /*
     * Source medium profile
     */
    public static StreamDimension sourceMediumDimension(@NotNull AtlasStream stream) {
        StreamDimension dim = new StreamDimension();
        dim.setName(InterfaceName.SourceMediumId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.addUsages(StreamDimension.Usage.Pivot);

        // hash source medium
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.SourceMedium.name());
        generator.setFromCatalog(false);
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
     * Retention: 120d
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
        stream.setRetentionDays(120);
        stream.setAttributeDerivers(getWebVisitAttrDerivers());
        return stream;
    }

    private static List<StreamAttributeDeriver> getWebVisitAttrDerivers() {
        List<StreamAttributeDeriver> derivers = new ArrayList<>();

        // total number of visit to target path
        StreamAttributeDeriver visitCount = new StreamAttributeDeriver();
        visitCount.setSourceAttributes(Collections.singletonList(InterfaceName.InternalId.name()));
        visitCount.setTargetAttribute(InterfaceName.TotalVisits.name());
        visitCount.setCalculation(StreamAttributeDeriver.Calculation.COUNT);
        derivers.add(visitCount);
        return derivers;
    }
}
