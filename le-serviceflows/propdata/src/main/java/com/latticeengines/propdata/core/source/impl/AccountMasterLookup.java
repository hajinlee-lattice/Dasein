package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMasterLookup")
public class AccountMasterLookup implements FixedIntervalSource {

    private static final long serialVersionUID = 3706653802200619595L;

    // 2 year duration in seconds
    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    @Autowired
    AccountMasterSeed baseSource;

    private long cutoffLimitInSeconds = DEFAULT_CUTOFF_LIMIT_IN_SECONDS;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "AccountMasterLookup";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID", "Key" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "accountMasterLookupRefreshService";
    }

    @Override
    public String getDirForBaseVersionLookup() {
        return "Snapshot";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
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
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public Long getCutoffDuration() {
        return cutoffLimitInSeconds;
    }
}
