package com.latticeengines.propdata.core.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;

@Component("semrushMostRecent")
public class SemrushMostRecent implements MostRecentSource, DomainBased, HasSqlPresence {

    private String cronExpression;

    @Autowired
    private Semrush baseSource;

    @Override
    public String getSourceName() {
        return "SemrushMostRecent";
    }

    @Override
    public String getSqlTableName() {
        return "Semrush_MostRecent";
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
        return TimeUnit.DAYS.toMillis(365 * 2);
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public String getSqlMatchDestination() {
        return "Semrush";
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
