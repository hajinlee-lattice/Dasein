package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMasterLookup")
public class AccountMasterLookup implements FixedIntervalSource {
    @Autowired
    AccountMasterSeed baseSource;

    private long cutoffLimitInSeconds;

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
        return new String[] { "LatticeID", "Domain", "DUNS" };
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
