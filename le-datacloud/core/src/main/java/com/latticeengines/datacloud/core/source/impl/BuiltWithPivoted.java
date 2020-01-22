package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("builtWithPivoted")
public class BuiltWithPivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = -7458296774400816711L;

    @Value("${propdata.job.builtwith.pivot.schedule:}")
    private String cronExpression;

    @Inject
    private BuiltWithMostRecent baseSource;

    @Override
    public String getSourceName() {
        return "BuiltWithPivoted";
    }

    @Override
    public String getSqlTableName() {
        return "BuiltWith_Pivoted_Source";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain" };
    }

    @Override
    public String getTimestampField() {
        return "Timestamp";
    }

    @Override
    public String getDomainField() {
        return "Domain";
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
        return "BuiltWith_Pivoted_Source";
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
