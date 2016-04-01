package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;

@Component("alexa")
public class Alexa implements CollectedSource {

    private static final long serialVersionUID = -7791007783522497933L;

    @Value("${propdata.job.alexa.archive.schedule:}")
    String cronExpression;

    @Override
    public String getSourceName() {
        return "Alexa";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "Alexa";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "URL", "LE_Last_Upload_Date" };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

}
