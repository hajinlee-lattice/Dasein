package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.LegacyDeleteTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_LEGACY_DELETE_TXFMR;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.LegacyDeleteJob;

@Component(TRANSFORMER_NAME)
public class LegacyDeleteTxfmr extends ConfigurableSparkJobTxfmr<LegacyDeleteJobConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_LEGACY_DELETE_TXFMR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<LegacyDeleteJobConfig>> getSparkJobClz() {
        return LegacyDeleteJob.class;
    }

    @Override
    protected Class<LegacyDeleteJobConfig> getJobConfigClz() {
        return LegacyDeleteJobConfig.class;
    }
}
