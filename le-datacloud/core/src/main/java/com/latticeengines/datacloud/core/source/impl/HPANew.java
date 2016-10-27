package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.CollectedSource;

@Component("hpaNew")
public class HPANew implements CollectedSource {

    private static final long serialVersionUID = -3425546227797626316L;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "HPANew";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "HPA_New";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "URL", "Feature", "LE_Last_Upload_Date" };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }
}
