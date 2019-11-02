package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_APPEND_RAWSTREAM;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.AppendRawStreamJob;

@Component(AppendRawStreamTxfmr.TRANSFORMER_NAME)
public class AppendRawStreamTxfmr extends ConfigurableSparkJobTxfmr<AppendRawStreamConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_APPEND_RAWSTREAM;

    @Override
    protected Class<AppendRawStreamConfig> getJobConfigClz() {
        return AppendRawStreamConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<AppendRawStreamConfig>> getSparkJobClz() {
        return AppendRawStreamJob.class;
    }
}
