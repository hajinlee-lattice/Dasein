package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("hpaNewPivoted")
public class HPANewPivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private String cronExpression;

    @Autowired
    private HPANewMostRecent baseSource;

    @Override
    public String getSourceName() {
        return "HPANewPivoted";
    }

    @Override
    public String getSqlTableName() {
        return "HPA_New_Pivoted_Source";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "URL" };
    }

    @Override
    public String getTimestampField() {
        return "Timestamp";
    }

    @Override
    public String getDomainField() {
        return "URL";
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
    public String getSqlMatchDestination() {
        return "HPA_New_Pivoted_Source";
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
