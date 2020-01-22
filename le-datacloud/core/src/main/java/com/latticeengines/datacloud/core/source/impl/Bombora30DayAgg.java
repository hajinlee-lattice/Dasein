package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("bombora30DayAgg")
public class Bombora30DayAgg implements DomainBased, DerivedSource, HasSqlPresence {

    private static final long serialVersionUID = -7716662820798495246L;

    private String cronExpression;

    @Inject
    private BomboraDepivoted baseSource;

    @Override
    public String getSourceName() {
        return "Bombora30DayAgg";
    }

    @Override
    public String getSqlTableName() {
        return "MadisonLogicAggregated_Source";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
    }

    @Override
    public String getTimestampField() {
        return "Date";
    }

    @Override
    public String getDomainField() {
        return "Domain";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public String getSqlMatchDestination() {
        return "MadisonLogicAggregated_Source";
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NEVER;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return null;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }
}
