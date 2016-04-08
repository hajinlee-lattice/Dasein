package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;

@Component
public class BuiltWith implements CollectedSource, HasSqlPresence {

    private static final long serialVersionUID = -1192390855705582815L;

    @Value("${propdata.job.buitwith.archive.schedule:}")
    private String cronExpression;

    @Override
    public String getSourceName() { return "BuiltWith"; }

    @Override
    public String getDownloadSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    public String getCollectedTableName() { return "BuiltWith"; }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String[] getPrimaryKey() { return new String[] { "Domain", "Technology_Name", "LE_Last_Upload_Date"  };  }

    @Override
    public String getDefaultCronExpression() { return cronExpression; }

    @Override
    public String getSqlTableName() { return getCollectedTableName(); }

    @Override
    public String getSqlMatchDestination() { return "BuiltWith_Source"; }

}