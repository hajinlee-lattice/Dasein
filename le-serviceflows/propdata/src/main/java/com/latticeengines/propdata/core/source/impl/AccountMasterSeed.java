package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMasterSeed")
public class AccountMasterSeed implements FixedIntervalSource {

    @Autowired
    LatticeCacheSeed baseSourceLattice;

    @Autowired
    DnBCacheSeed baseSourceDnB;

    private long cutoffLimitInSeconds;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "AccountMasterSeed";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSourceLattice, baseSourceDnB };
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "accountMasterSeedRefreshService";
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
