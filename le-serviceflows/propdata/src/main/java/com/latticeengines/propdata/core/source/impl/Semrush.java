package com.latticeengines.propdata.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;

@Component("semrush")
public class Semrush implements CollectedSource {

    private static final long serialVersionUID = -437395685948895897L;
    private String cronExpression;

    @Override
    public String getSourceName() {
        return "Semrush";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "Semrush";
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
