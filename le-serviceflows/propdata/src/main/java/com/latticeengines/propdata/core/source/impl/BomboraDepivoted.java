package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("bomboraDepivoted")
public class BomboraDepivoted implements FixedIntervalSource {

    private static final long serialVersionUID = 2471824706529427531L;

    // 2 year duration in seconds
    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    @Value("${propdata.job.bomboradepivoted.fixedinterval.schedule:0 0 15 * * *}")
    private String cronExpression;

    @Value("${propdata.job.bomboradepivoted.fixedinterval.cutoffLimitInSeconds:" + DEFAULT_CUTOFF_LIMIT_IN_SECONDS
            + "}")
    private long cutoffLimitInSeconds;

    @Autowired
    private BomboraFirehose baseSource;

    @Override
    public Source[] getBaseSources() {
        return new Source[] { baseSource };
    }

    @Override
    public String getSourceName() {
        return "BomboraDepivoted";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "ID" };
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

    @Override
    public String getDirForBaseVersionLookup() {
        return "Raw";
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "bomboraDepivotedService";
    }

    @Override
    public Long getCutoffDuration() {
        return cutoffLimitInSeconds;
    }
}
