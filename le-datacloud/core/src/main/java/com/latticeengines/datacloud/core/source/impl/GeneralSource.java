package com.latticeengines.datacloud.core.source.impl;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

/*
 * This is place holder for data sources created on the fly.
 */
public class GeneralSource implements DerivedSource {

    private static final long serialVersionUID = 603829385601451985L;

    String sourceName = null;
    /*
     * name of the source
     */
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public Source[] getBaseSources() {
        return null;
    }

    @Override
    public PurgeStrategy getPurgeStrategy() {
        return PurgeStrategy.NUM_VERSIONS;
    }

    /*
     * timestamp field for sorting
     */
    public String getTimestampField() {
        return "Timestamp";
    }

    /*
     * primary key
     */
    public String[] getPrimaryKey() {
        return null;
    }

    /*
     * cron expression used to specify frequency of source data engine for this
     * source
     */
    public String getDefaultCronExpression() {
         return null;
    }

    @Override
    public Integer getNumberOfVersionsToKeep() {
        return 1;
    }

    @Override
    public Integer getNumberOfDaysToKeep() {
        return null;
    }

}
