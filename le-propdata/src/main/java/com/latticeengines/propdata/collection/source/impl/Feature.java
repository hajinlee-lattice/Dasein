package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;

@Component("feature")
public class Feature implements CollectedSource {

    private static final long serialVersionUID = 2079061038810691592L;

    @Value("${propdata.job.feature.schedule:}")
    String cronExpression;

    @Override
    public String getSourceName() { return "Feature"; }

    @Override
    public String getRefreshServiceBean() { return "featureArchiveService"; }

    @Override
    public String getDownloadSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    public String getCollectedTableName() {
        return "Feature";
    }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String[] getPrimaryKey() { return new String[] { "URL", "Feature", "LE_Last_Upload_Date"  };  }

    @Override
    public String getCronExpression() { return cronExpression; }
}