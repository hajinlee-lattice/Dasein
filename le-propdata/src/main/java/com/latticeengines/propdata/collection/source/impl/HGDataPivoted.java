package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

@Component("hgDataPivoted")
public class HGDataPivoted implements PivotedSource, DomainBased {

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
    public String getRefreshServiceBean() { return "hgDataPivotService"; }

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
