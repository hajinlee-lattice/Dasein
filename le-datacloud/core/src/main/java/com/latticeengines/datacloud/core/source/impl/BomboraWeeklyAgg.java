package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("bomboraWeeklyAgg")
public class BomboraWeeklyAgg implements DerivedSource {

    private static final long serialVersionUID = 8420797405204476273L;

    @Inject
    private BomboraDepivoted bomboraDepivoted;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { bomboraDepivoted };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "BomboraWeeklyAgg";
    }

    @Override
    public String getTimestampField() {
        return "Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "DomainID" };
    }

    @Override
    public String getDefaultCronExpression() {
        return null;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 1;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }

}
