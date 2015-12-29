package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

@Component("featurePivoted")
public class FeaturePivoted implements PivotedSource, DomainBased {

    private static final long serialVersionUID = -1456685001590154145L;

    @Autowired
    FeatureMostRecent baseSource;

    @Override
    public String getSourceName() { return "FeaturePivoted"; }

    @Override
    public String getSqlTableName() { return "Feature_Pivoted_Source"; }

    @Override
    public String getRefreshServiceBean() { return "featurePivotService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "URL" }; }

    @Override
    public String getTimestampField() { return "Timestamp"; }

    @Override
    public String getDomainField() {  return "Domain"; }

    @Override
    public Source getBaseSource() { return baseSource; }

}
