package com.latticeengines.propdata.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("publicDomain")
public class PublicDomain implements DerivedSource {

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "PublicDomain";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "PublicDomain" };
    }

    @Override
    public String getTimestampField() {
        return null;
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] {};
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
        return 3;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return 7;
    }
}