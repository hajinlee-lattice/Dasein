package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("dnBCacheSeed")
public class DnBCacheSeed implements FixedIntervalSource {

    private static final long serialVersionUID = -6280748201445659077L;

    // 2 year duration in seconds
    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    private String cronExpression;

    private long cutoffLimitInSeconds = DEFAULT_CUTOFF_LIMIT_IN_SECONDS;

    @Autowired
    private DnBCacheSeedRaw baseSource;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getSourceName() {
        return "DnBCacheSeed";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "DUNS_NUMBER", "LE_DOMAIN" };
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
        return 3;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return 7;
    }

    @Override
    public String getDirForBaseVersionLookup() {
        return "Raw";
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "dnbCacheSeedCleanService";
    }

    @Override
    public Long getCutoffDuration() {
        return cutoffLimitInSeconds;
    }
}
