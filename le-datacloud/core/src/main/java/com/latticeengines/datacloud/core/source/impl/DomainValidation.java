package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.RefreshedSource;
import com.latticeengines.datacloud.core.source.Source;

@Component("domainValidation")
public class DomainValidation implements RefreshedSource {

    private static final long serialVersionUID = -7130579631737299357L;

    @Inject
    private LatticeCacheSeed latticeCacheSeed;

    private String cronExpression;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { latticeCacheSeed };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NEVER;
    }

    @Override
    public String getSourceName() {
        return "DomainValidation";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Update_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
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
