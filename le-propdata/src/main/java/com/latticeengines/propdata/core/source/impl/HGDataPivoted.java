package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.Source;

@Component
public class HGDataPivoted implements PivotedSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = 5193097838348800451L;

    @Value("${propdata.job.hgdata.pivot.schedule:}")
    String cronExpression;

    @Autowired
    HGData baseSource;

    @Override
    public String getSourceName() { return "HGDataPivoted"; }

    @Override
    public String getSqlTableName() { return "HGData_Pivoted_Source"; }

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

    @Override
    public String getSqlMatchDestination() { return "HGData_Pivoted_Source"; }

}
