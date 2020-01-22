package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("accountMasterSeedMerged")
public class AccountMasterSeedMerged implements DerivedSource {

    private static final long serialVersionUID = -3119903346347156027L;

    @Inject
    LatticeCacheSeed latticeCacheSeed;

    @Inject
    DnBCacheSeed dnBCacheSeed;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { dnBCacheSeed, latticeCacheSeed };
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public String getSourceName() {
        return "AccountMasterSeedMerged";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeID" };
    }

    @Override
    public String getDefaultCronExpression() {
        return null;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 1;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }

    public String getDomainField() {
        return "Domain";
    }

    public String getDunsField() {
        return "DUNS";
    }

}
