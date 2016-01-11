package com.latticeengines.propdata.collection.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.MostRecentSource;

@Component("builtWithMostRecent")
public class BuiltWithMostRecent implements MostRecentSource, DomainBased {

    private static final long serialVersionUID = -3304714347997988410L;

    @Value("${propdata.job.buitwith.refresh.schedule:}")
    String cronExpression;

    @Autowired
    BuiltWith baseSource;

    @Override
    public String getSourceName() { return "BuiltWithMostRecent"; }

    @Override
    public String getSqlTableName() { return "BuiltWith_MostRecent"; }

    @Override
    public String getRefreshServiceBean() { return "builtWithRefreshService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "Domain", "Technology_Name" }; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String getDomainField() {  return "Domain"; }

    @Override
    public CollectedSource[] getBaseSources() { return new CollectedSource[]{ baseSource }; }

    @Override
    public Long periodToKeep() {  return TimeUnit.DAYS.toMillis(365); }

    @Override
    public String getDefaultCronExpression() { return cronExpression; }

}