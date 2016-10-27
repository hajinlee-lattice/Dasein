package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.CollectedSource;

@Component("unmatchedSource")
public class UnmatchedSource implements CollectedSource {

    private static final long serialVersionUID = -689027009730594994L;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "UnmatchedSource";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "UnmatchedSource";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "Domain", "LE_Last_Upload_Date" };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

}
