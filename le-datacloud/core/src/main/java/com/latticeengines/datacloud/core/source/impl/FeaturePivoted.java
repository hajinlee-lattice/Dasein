package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

@Component("featurePivoted")
public class FeaturePivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = -1456685001590154145L;

    @Value("${propdata.job.feature.pivot.schedule:}")
    private String cronExpression;

    @Inject
    private FeatureMostRecent baseSource;

    @Override
    public String getSourceName() {
        return "FeaturePivoted";
    }

    @Override
    public String getSqlTableName() {
        return "Feature_Pivoted_Source";
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
        return "Feature_Pivoted_Source";
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
