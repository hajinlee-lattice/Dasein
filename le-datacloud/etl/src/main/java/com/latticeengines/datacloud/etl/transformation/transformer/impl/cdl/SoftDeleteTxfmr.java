package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.SoftDeleteTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.SoftDeleteJob;

@Component(TRANSFORMER_NAME)
public class SoftDeleteTxfmr extends ConfigurableSparkJobTxfmr<SoftDeleteConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_SOFT_DELETE_TXFMR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<SoftDeleteConfig>> getSparkJobClz() {
        return SoftDeleteJob.class;
    }

    @Override
    protected Class<SoftDeleteConfig> getJobConfigClz() {
        return SoftDeleteConfig.class;
    }
}
