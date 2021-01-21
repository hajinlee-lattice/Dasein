package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.SoftDeleteTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_ENRICH_WEBVISIT;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.EnrichWebVisitJob;

@Component(TRANSFORMER_NAME)
public class EnrichWebVisitTxfmr extends ConfigurableSparkJobTxfmr<EnrichWebVisitJobConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_ENRICH_WEBVISIT;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<EnrichWebVisitJobConfig>> getSparkJobClz() {
        return EnrichWebVisitJob.class;
    }

    @Override
    protected Class<EnrichWebVisitJobConfig> getJobConfigClz() {
        return EnrichWebVisitJobConfig.class;
    }
}
