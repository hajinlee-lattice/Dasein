package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.source.Source;

/*
 * This is place holder for data sources created on the fly.
 */
@Component("pipelineSource")
public class PipelineSource implements DerivedSource {

    private static final long serialVersionUID = -3425546227797626315L;

    String sourceName = "pipelineSource";
    /*
     * name of the source
     */
    public String getSourceName() {
        return sourceName;
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
