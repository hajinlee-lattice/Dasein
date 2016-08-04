package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("latticeCacheSeed")
public class LatticeCacheSeed implements DerivedSource {

    private String cronExpression;

    @Autowired
    private UnmatchedSource baseSource;

    @Override
    public String getSourceName() {
        return "LatticeCacheSeed";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain", "Name", "State", "Country" };
    }

    @Override
    public String getTimestampField() {
        return null;
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
