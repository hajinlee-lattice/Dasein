package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;

@Component
public class OrbIntelligence implements CollectedSource {

    private static final long serialVersionUID = 3359238491845056238L;

    @Value("${propdata.job.orb.archive.schedule:}")
    private String cronExpression;

    @Override
    public String getSourceName() {
        return "OrbIntelligence";
    }

    @Override
    public String getDownloadSplitColumn() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getCollectedTableName() {
        return "OrbIntelligence";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "orb_num", "LE_Last_Upload_Date" };
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }
}