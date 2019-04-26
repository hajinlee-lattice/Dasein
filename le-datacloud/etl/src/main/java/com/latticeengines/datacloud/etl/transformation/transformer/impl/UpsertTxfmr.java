package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_UPSERT_TXMFR;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.spark.exposed.job.common.UpsertJob;


@Component(UpsertTxfmr.TRANSFORMER_NAME)
public class UpsertTxfmr extends ConfigurableSparkJobTxfmr<UpsertConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_UPSERT_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<UpsertJob> getSparkJobClz() {
        return UpsertJob.class;
    }

    @Override
    protected Class<UpsertConfig> getJobConfigClz() {
        return UpsertConfig.class;
    }

}
