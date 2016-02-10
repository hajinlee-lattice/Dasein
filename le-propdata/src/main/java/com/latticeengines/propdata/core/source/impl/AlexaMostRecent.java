package com.latticeengines.propdata.core.source.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.MostRecentSource;

@Component("alexaMostRecent")
public class AlexaMostRecent implements MostRecentSource, DomainBased, HasSqlPresence {

    private static final long serialVersionUID = 5703265655999503611L;

    @Value("${propdata.job.alexa.refresh.schedule:}")
    String cronExpression;

    @Autowired
    Alexa baseSource;

    @Override
    public String getSourceName() {
        return "AlexaMostRecent";
    }

    @Override
    public String getSqlTableName() {
        return "Alexa_MostRecent";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "URL" };
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getDomainField() {
        return "URL";
    }

    @Override
    public CollectedSource[] getBaseSources() {
        return new CollectedSource[] { baseSource };
    }

    @Override
    public Long periodToKeep() {
        return TimeUnit.DAYS.toMillis(365);
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public String getSqlMatchDestination() {
        return "Alexa_Source";
    }

}
