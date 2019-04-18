package com.latticeengines.datacloud.core.source.impl;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;

@Component("builtWithMostRecent")
public class BuiltWithMostRecent implements MostRecentSource, DomainBased {

    private static final long serialVersionUID = -3304714347997988410L;

    @Value("${propdata.job.buitwith.refresh.schedule:}")
    private String cronExpression;

    @Inject
    private BuiltWith baseSource;

    @Override
    public String getSourceName() {
        return "BuiltWithMostRecent";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain", "Technology_Name" };
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getDomainField() {
        return "Domain";
    }

    @Override
    public CollectedSource[] getBaseSources() {
        return new CollectedSource[] { baseSource };
    }

    @Override
    public Long periodToKeep() {
        return TimeUnit.DAYS.toMillis(365);
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 2;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return 7;
    }

}
