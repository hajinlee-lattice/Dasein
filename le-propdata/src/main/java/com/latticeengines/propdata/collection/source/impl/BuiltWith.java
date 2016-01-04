package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.CollectedSource;

@Component("builtWith")
public class BuiltWith implements CollectedSource {

    private static final long serialVersionUID = -1192390855705582815L;

    @Value("${propdata.job.buitwith.schedule:}")
    String cronExpression;

    @Override
    public String getSourceName() { return "BuiltWith"; }

    @Override
    public String getRefreshServiceBean() { return "builtWithArchiveService"; }

    @Override
    public String getDownloadSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    public String getCollectedTableName() { return "BuiltWith"; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String[] getPrimaryKey() { return new String[] { "Domain", "Technology_Name", "LE_Last_Upload_Date"  };  }

    @Override
    public String getCronExpression() { return cronExpression; }

}