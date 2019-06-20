package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("orbCompanyRaw")
public class OrbCompanyRaw implements DerivedSource {

    private static final long serialVersionUID = -2367515837904364398L;

    private IngestionSource baseSource = new IngestionSource(IngestionNames.ORB_INTELLIGENCE);

    @Override
    public String getSourceName() {
        return "OrbCompanyRaw";
    }

    @Override
    public String getTimestampField() {
        return null;
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "OrbNum" };
    }

    @Override
    public String getDefaultCronExpression() {
        return null;
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
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
