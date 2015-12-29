package com.latticeengines.propdata.collection.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.DomainBasedSource;

@Component("builtWithSource")
public class BuiltWithSource implements DomainBasedSource, CollectedSource {

    private static final long serialVersionUID = -1192390855705582815L;

    @Override
    public String getSourceName() { return "BuiltWith"; }

    @Override
    public String getSqlTableName() { return "BuiltWith_MostRecent"; }

    @Override
    public String getRefreshServiceBean() { return "builtWithArchiveService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{"Domain", "Technology_Name"}; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String getDomainField() {  return "Domain"; }

    @Override
    public String getCollectedTableName() {
        return "BuiltWith";
    }

}