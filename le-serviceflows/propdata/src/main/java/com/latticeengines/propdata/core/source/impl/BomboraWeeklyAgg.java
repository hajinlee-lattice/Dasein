package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("bomboraWeeklyAgg")
public class BomboraWeeklyAgg implements DerivedSource {

    private static final long serialVersionUID = 8420797405204476273L;

    @Autowired
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
