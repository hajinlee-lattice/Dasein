package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_SYSTEM_BATCH_TXMFR;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.spark.exposed.job.cdl.MergeSystemBatchJob;

@Component(MergeSystemBatchTxfmr.TRANSFORMER_NAME)
public class MergeSystemBatchTxfmr extends ConfigurableSparkJobTxfmr<MergeSystemBatchConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_MERGE_SYSTEM_BATCH_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<MergeSystemBatchJob> getSparkJobClz() {
        return MergeSystemBatchJob.class;
    }

    @Override
    protected Class<MergeSystemBatchConfig> getJobConfigClz() {
        return MergeSystemBatchConfig.class;
    }

}
