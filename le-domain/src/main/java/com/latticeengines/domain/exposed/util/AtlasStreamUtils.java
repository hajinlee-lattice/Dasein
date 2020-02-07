package com.latticeengines.domain.exposed.util;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

public class AtlasStreamUtils {

    protected AtlasStreamUtils() {
        throw new UnsupportedOperationException();
    }

    /*-
     * Stream object for entityType data
     *
     * Period: Week
     * Retention: 1yr
     * Entity: Account
     * Rollup: Unique account/contact visit, All account/contact visit
     */
    public static AtlasStream newAtlasStream(@NotNull Tenant tenant, @NotNull DataFeedTask dataFeedTask,
                                             @NotNull String streamName, @NotNull List<String> matchEntities,
                                             @NotNull List<String> aggrEntities, @NotNull String dateAttribute,
                                             List<String> periods, Integer retentionDay) {
        AtlasStream stream = new AtlasStream();
        stream.setName(streamName);
        stream.setTenant(tenant);
        stream.setDataFeedTask(dataFeedTask);
        stream.setMatchEntities(matchEntities);
        stream.setAggrEntities(aggrEntities);
        stream.setDateAttribute(dateAttribute);
        stream.setPeriods(CollectionUtils.isEmpty(periods) ?
                Collections.singletonList(PeriodStrategy.Template.Week.name()) : periods);
        stream.setRetentionDays(retentionDay == null ? 365 : retentionDay);
        return stream;
    }

    /*
     * profile
     */
    public static StreamDimension createHashDimension(@NotNull AtlasStream stream, Catalog catalog,
                                                        String dimensionName, StreamDimension.Usage usage,
                                                      String attributeName) {
        StreamDimension dim = new StreamDimension();
        dim.setName(dimensionName);
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.addUsages(usage);
        dim.setCatalog(catalog);

        // hash
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(attributeName);
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setName(attributeName);
        calculator.setAttribute(attributeName);
        dim.setCalculator(calculator);
        return dim;
    }

    public static Catalog createCatalog(Tenant tenant, String catalogName, DataFeedTask dataFeedTask) {
        Catalog catalog = new Catalog();
        catalog.setTenant(tenant);
        catalog.setName(catalogName);
        catalog.setCatalogId(Catalog.generateId());
        catalog.setDataFeedTask(dataFeedTask);
        return catalog;
    }
}
