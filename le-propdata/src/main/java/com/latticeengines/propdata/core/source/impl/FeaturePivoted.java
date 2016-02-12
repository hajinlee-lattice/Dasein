package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("featurePivoted")
public class FeaturePivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = -1456685001590154145L;

    @Value("${propdata.job.feature.pivot.schedule:}")
    String cronExpression;

    @Autowired
    FeatureMostRecent baseSource;

    @Override
    public String getSourceName() { return "FeaturePivoted"; }

    @Override
    public String getSqlTableName() { return "Feature_Pivoted_Source"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "URL" }; }

    @Override
    public String getTimestampField() { return "Timestamp"; }

    @Override
    public String getDomainField() {  return "URL"; }

    @Override
    public Source[] getBaseSources() { return new Source[] { baseSource }; }

    @Override
    public String getDefaultCronExpression() { return cronExpression; }

    @Override
    public String getSqlMatchDestination() { return "Feature_Pivoted_Source"; }

    @Override
    public PurgeStrategy getPurgeStrategy() {  return PurgeStrategy.NUM_VERSIONS; }

    @Override
    public Integer getNumberOfVersionsToKeep() { return 2; }

    @Override
    public Integer getNumberOfDaysToKeep() { return 7; }

}
