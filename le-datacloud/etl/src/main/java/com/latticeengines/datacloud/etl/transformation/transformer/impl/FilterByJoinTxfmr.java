package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_FILTER_BY_JOIN_TXFMR;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig;
import com.latticeengines.spark.exposed.job.common.FilterByJoinJob;

@Component(FilterByJoinTxfmr.TRANSFORMER_NAME)
public class FilterByJoinTxfmr extends ConfigurableSparkJobTxfmr<FilterByJoinConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_FILTER_BY_JOIN_TXFMR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<FilterByJoinJob> getSparkJobClz() {
        return FilterByJoinJob.class;
    }

    @Override
    protected Class<FilterByJoinConfig> getJobConfigClz() {
        return FilterByJoinConfig.class;
    }

}
