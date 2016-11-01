package com.latticeengines.datacloud.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.DunsBased;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("dnBCacheSeed")
public class DnBCacheSeed implements DerivedSource, DomainBased, DunsBased {

    private static final long serialVersionUID = -7331679821512614934L;
    
    private String cronExpression;

    @Autowired
    private DnBCacheSeedRaw baseSource;

    @Override
    public String getSourceName() {
        return "DnBCacheSeed";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getDunsField() {
        return "DUNS_NUMBER";
    }

    @Override
    public String getDomainField() {
        return "LE_DOMAIN";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "DUNS_NUMBER", "LE_DOMAIN" };
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
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

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }
}
