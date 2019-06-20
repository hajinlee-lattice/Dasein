package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("orbDomainRaw")
public class OrbDomainRaw implements DerivedSource {

    private static final long serialVersionUID = 3578974727605909373L;

    private IngestionSource baseSource = new IngestionSource(IngestionNames.ORB_INTELLIGENCE);

    @Override
    public String getSourceName() {
        return "OrbDomainRaw";
    }

    @Override
    public String getTimestampField() {
        return null;
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "OrbNum", "WebDomain" };
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
