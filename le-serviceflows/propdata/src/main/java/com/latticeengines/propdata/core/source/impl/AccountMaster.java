package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("accountMaster")
public class AccountMaster implements DerivedSource {

    private static final long serialVersionUID = 5343511350388581523L;

    @Autowired
    AccountMasterSeed baseSource;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "AccountMaster";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LatticeId" };
    }

    @Override
    public String getTimestampField() {
        return null;
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NEVER;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 3;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return 7;
    }
}