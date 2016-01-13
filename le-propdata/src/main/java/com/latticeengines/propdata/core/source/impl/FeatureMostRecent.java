package com.latticeengines.propdata.core.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.MostRecentSource;

@Component("featureMostRecent")
public class FeatureMostRecent implements MostRecentSource, DomainBased {

    private static final long serialVersionUID = 3483355190999074200L;

    @Value("${propdata.job.feature.refresh.schedule:}")
    String cronExpression;

    @Autowired
    Feature baseSource;

    @Override
    public String getSourceName() { return "FeatureMostRecent"; }

    @Override
    public String getSqlTableName() { return "Feature_MostRecent"; }

    @Override
    public String getRefreshServiceBean() { return "featureRefreshService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "URL", "Feature" }; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String getDomainField() {  return "URL"; }

    @Override
    public CollectedSource[] getBaseSources() { return new CollectedSource[] { baseSource }; }

    @Override
    public Long periodToKeep() {  return TimeUnit.DAYS.toMillis(365); }

    @Override
    public String getDefaultCronExpression() { return cronExpression; }

}