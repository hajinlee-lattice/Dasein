package com.latticeengines.datacloud.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("orbCacheSeedSecondaryDomain")
public class OrbCacheSeedSecondaryDomain implements DerivedSource {

    private static final long serialVersionUID = -198684822043409093L;

    private String cronExpression;

    @Autowired
    private UnmatchedSource baseSource;

    @Override
    public String getSourceName() {
        return "OrbCacheSeedSecondaryDomain";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "SecondaryDomain" };
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
