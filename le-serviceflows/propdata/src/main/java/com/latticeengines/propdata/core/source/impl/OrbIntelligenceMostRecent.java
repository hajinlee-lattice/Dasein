package com.latticeengines.propdata.core.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;

@Component("orbIntelligenceMostRecent")
public class OrbIntelligenceMostRecent implements MostRecentSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = -7492688545254273100L;

    @Value("${propdata.job.orb.refresh.schedule:}")
    private String cronExpression;

    @Autowired
    private OrbIntelligence baseSource;

    @Override
    public String getSourceName() {
        return "OrbIntelligenceMostRecent";
    }

    @Override
    public String getSqlTableName() {
        return "OrbIntelligence_MostRecent";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
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
    public String getSqlMatchDestination() {
        return "OrbIntelligence_Source";
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