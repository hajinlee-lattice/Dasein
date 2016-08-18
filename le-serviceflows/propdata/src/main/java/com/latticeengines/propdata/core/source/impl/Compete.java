package com.latticeengines.propdata.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;

@Component("compete")
public class Compete implements CollectedSource {

    private static final long serialVersionUID = 1656423386846596128L;
    private String cronExpression;

    @Override
    public String getSourceName() {
        return "Compete";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "Compete";
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
