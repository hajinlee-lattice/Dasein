package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("hpaNewPivoted")
public class HPANewPivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = -6154293757323120037L;

    private String cronExpression;

    @Inject
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
