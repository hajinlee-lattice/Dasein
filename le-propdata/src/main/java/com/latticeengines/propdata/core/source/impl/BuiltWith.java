package com.latticeengines.propdata.core.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.MostRecentSource;

@Component("builtWith")
public class BuiltWith implements MostRecentSource, DomainBased {

    private static final long serialVersionUID = -3304714347997988410L;

    @Value("${propdata.job.buitwith.refresh.schedule:}")
    String cronExpression;

    @Autowired
    BuiltWithRaw baseSource;

    @Override
    public String getSourceName() { return "BuiltWith"; }

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