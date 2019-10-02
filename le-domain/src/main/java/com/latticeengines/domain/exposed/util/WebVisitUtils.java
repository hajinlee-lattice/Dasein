package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
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

    /*-
     * PathPatternId dimension for web visit
     */
    public static StreamDimension newWebVisitDimension(@NotNull AtlasStream stream, Catalog pathPtnCatalog) {
        StreamDimension dim = new StreamDimension();
        dim.setName(InterfaceName.PathPatternId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.setCatalog(pathPtnCatalog);

        // standardize and hash ptn name for dimension
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.PathPatternName.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);
        // use url attr in stream to determine whether it matches catalog pattern
        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
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

        // number of unique accounts visit target path
        StreamAttributeDeriver uniqueAccountVisitCount = new StreamAttributeDeriver();
        uniqueAccountVisitCount.setSourceAttributes(Collections.singletonList(InterfaceName.AccountId.name()));
        uniqueAccountVisitCount.setTargetAttribute(InterfaceName.TotalUniqueAccountVisits.name());
        uniqueAccountVisitCount.setCalculation(StreamAttributeDeriver.Calculation.DISTINCT_COUNT);
        derivers.add(uniqueAccountVisitCount);
        return derivers;
    }
}
