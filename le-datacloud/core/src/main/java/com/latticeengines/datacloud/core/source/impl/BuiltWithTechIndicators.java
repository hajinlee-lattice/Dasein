package com.latticeengines.datacloud.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("builtWithTechIndicators")
public class BuiltWithTechIndicators implements DerivedSource, DomainBased {

    private static final long serialVersionUID = 603829385601451921L;

    @Autowired
    private BuiltWithMostRecent builtWithMostRecent;

    @Override
    public Source[] getBaseSources() {
        return new Source[]{ builtWithMostRecent };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "BuiltWithTechIndicators";
    }

    @Override
    public String getTimestampField() {
        return "Timestamp";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
    }

    @Override
    public String getDomainField() {
        return "Domain";
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
