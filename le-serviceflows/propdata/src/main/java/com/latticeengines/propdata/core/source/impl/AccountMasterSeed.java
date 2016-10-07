package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMasterSeed")
public class AccountMasterSeed implements FixedIntervalSource {

    private static final long serialVersionUID = -3336333169415624688L;

    // 2 year duration in seconds
    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    @Autowired
    LatticeCacheSeed baseSourceLattice;

    @Autowired
    DnBCacheSeed baseSourceDnB;

    private long cutoffLimitInSeconds = DEFAULT_CUTOFF_LIMIT_IN_SECONDS;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "AccountMasterSeed";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSourceDnB, baseSourceLattice };
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "accountMasterSeedRebuildService";
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

    public String getDomainField() {
        return "Domain";
    }

    public String getDunsField() {
        return "DUNS";
    }

    public String[] getRetainFields() {
        return new String[] { "LatticeID", "DUNS", "Domain", "Name", "Street", "City", "State", "Country", "ZipCode",
                "LE_IS_PRIMARY_DOMAIN", "LE_IS_PRIMARY_LOCATION", "LE_NUMBER_OF_LOCATIONS" };
    }
}
