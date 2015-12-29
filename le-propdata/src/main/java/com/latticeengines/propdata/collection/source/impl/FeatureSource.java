package com.latticeengines.propdata.collection.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.DomainBasedSource;

@Component("featureSource")
public class FeatureSource implements DomainBasedSource, CollectedSource {

    private static final long serialVersionUID = 2079061038810691592L;

    @Override
    public String getSourceName() { return "Feature"; }

    @Override
    public String getSqlTableName() { return "Feature_MostRecent"; }

    @Override
    public String getRefreshServiceBean() { return "featureArchiveService"; }

    @Override
    public String[] getPrimaryKey() { return new String[]{"URL", "Feature"}; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String getDomainField() {  return "URL"; }

    @Override
    public String getCollectedTableName() {
        return "Feature";
    }

}