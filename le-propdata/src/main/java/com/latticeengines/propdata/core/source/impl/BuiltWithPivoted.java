package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.Source;

@Component("builtWithPivoted")
public class BuiltWithPivoted implements PivotedSource, DomainBased {

    private static final long serialVersionUID = -7458296774400816711L;

    @Value("${propdata.job.builtwith.pivot.schedule:}")
    String cronExpression;

    @Autowired
    BuiltWithMostRecent baseSource;

    @Override
    public String getSourceName() { return "BuiltWithPivoted"; }

    @Override
    public String getSqlTableName() { return "BuiltWith_Pivoted_Source"; }

    @Override
    public String getRefreshServiceBean() { return "builtWithPivotService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "Domain" }; }

    @Override
    public String getTimestampField() { return "Timestamp"; }

    @Override
    public String getDomainField() {  return "Domain"; }

    @Override
    public Source[] getBaseSources() { return new Source[] { baseSource }; }

    @Override
    public String getDefaultCronExpression() { return cronExpression; }

}
