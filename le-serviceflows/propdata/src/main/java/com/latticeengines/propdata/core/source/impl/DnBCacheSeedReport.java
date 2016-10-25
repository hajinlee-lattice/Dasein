package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.CharacterizationSource;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.source.Source;

@Component("dnBCacheSeedReport")
public class DnBCacheSeedReport implements CharacterizationSource {

    private static final long serialVersionUID = 5343511350388581523L;

    private static final long DEFAULT_CUTOFF_LIMIT_IN_SECONDS = 2 * 366 * 24 * 60 * 60L;

    @Autowired
    DnBCacheSeed dnBCacheSeed;

    private long cutoffLimitInSeconds = DEFAULT_CUTOFF_LIMIT_IN_SECONDS;

    private String cronExpression;

    @Override
    public String getSourceName() {
        return "DnBCacheSeedReport";
    }

    @Override
    public Source[] getBaseSources() {
        return new Source[] { dnBCacheSeed };
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "LE_COUNTRY", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE", "LE_NUMBER_OF_LOCATIONS", "Version"};
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "accountMasterReportService";
    }

    @Override
    public String getDirForBaseVersionLookup() {
        return "Snapshot";
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 3;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return 7;
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    @Override
    public Long getCutoffDuration() {
        return cutoffLimitInSeconds;
    }

    public void setCutoffDuration(Long cutoffLimitInSeconds) {
        this.cutoffLimitInSeconds = cutoffLimitInSeconds;
    }

    @Override
    public String getVersionKey() {
        return "Version";
    }

    @Override
    public String[] getAttrKey() {
        return new String[] {"AttrCount1", "AttrCount2", "AttrCount3", "AttrCount4"}; 
    }

    @Override
    public String getTotalKey() {
        return "GroupTotal";
    }

    @Override
    public String[] getGroupKeys() {
        return new String[] {"LE_COUNTRY", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE",  "LE_NUMBER_OF_LOCATIONS"};
    }

    @Override
    public String[] getExcludeAttrs() {
        return new String[] {"LE_COUNTRY", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE",  "LE_NUMBER_OF_LOCATIONS"};
    }
}
