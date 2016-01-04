package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

@Component("testPivotedSource")
public class TestPivotedSource implements PivotedSource, DomainBased {

    private static final long serialVersionUID = -976817973820431173L;

    @Autowired
    TestCollectedSource baseSource;

    @Override
    public String getSourceName() { return "TestPivoted"; }

    @Override
    public String getSqlTableName() { return "TestPivotedSource"; }

    @Override
    public String getRefreshServiceBean() { return "testPivotService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{ "URL" }; }

    @Override
    public String getTimestampField() { return "Timestamp"; }

    @Override
    public String getDomainField() {  return "Domain"; }

    @Override
    public Source[] getBaseSources() { return new Source[] { baseSource }; }

    @Override
    public String getCronExpression() { return null; }

}
