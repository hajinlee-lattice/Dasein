package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.DunsBased;
import com.latticeengines.datacloud.core.source.FixedIntervalSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("accountMaster")
public class AccountMaster implements DomainBased, DunsBased, FixedIntervalSource {

    private static final long serialVersionUID = 5343511350388581523L;

    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    private static final String MAJOR_VERSION = "2.0";

    @Inject
    AccountMasterSeed accountMasterSeed;

    private long cutoffLimitInSeconds = DEFAULT_CUTOFF_LIMIT_IN_SECONDS;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "AccountMaster";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { accountMasterSeed };
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "accountMasterRebuildService";
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

    public void setCutoffDuration(Long cutoffLimitInSeconds) {
        this.cutoffLimitInSeconds = cutoffLimitInSeconds;
    }

    public String getDomainField() {
        return "Domain";
    }

    public String getDunsField() {
        return "DUNS";
    }

    public String getMajorVersion() {
        return MAJOR_VERSION;
    }
}
