package com.latticeengines.datacloud.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("bomboraSurgePivoted")
public class BomboraSurgePivoted implements DerivedSource, DomainBased {

    private static final long serialVersionUID = -4970128975902663484L;

    @Autowired
    private GeneralSource bomboraSurge = new GeneralSource("BomboraSurge");

    @Override
    public Source[] getBaseSources() {
        return new Source[] { bomboraSurge };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "BomboraSurgePivoted";
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
