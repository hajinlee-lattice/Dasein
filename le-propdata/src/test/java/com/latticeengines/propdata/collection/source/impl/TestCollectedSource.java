package com.latticeengines.propdata.collection.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.DomainBased;

@Component("testCollectedSource")
public class TestCollectedSource implements DomainBased, CollectedSource {


    private static final long serialVersionUID = -1113746323283646177L;

    @Override
    public String getSourceName() { return "TestCollected"; }

    @Override
    public String getRefreshServiceBean() { return "testArchiveService"; }

    @Override
    public String getDomainField() {  return "URL"; }

    @Override
    public String getCollectedTableName() {
        return "TestCollected";
    }

    @Override
    public String getDownloadSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{}; }

    @Override
    public String getCronExpression() { return null; }

}