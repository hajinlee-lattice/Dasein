package com.latticeengines.propdata.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("hgDataClean")
public class HGDataClean implements DerivedSource {

    private static final long serialVersionUID = 603829385601451985L;

    @Override
    public Source[] getBaseSources() {
        return new Source[0];
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "HGDataClean";
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
