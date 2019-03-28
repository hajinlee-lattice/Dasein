package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.RemoveOrphanTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_REMOVE_ORPHAN_CONTACT;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.RemoveOrphanJob;


@Component(TRANSFORMER_NAME)
public class RemoveOrphanTxfmr extends ConfigurableSparkJobTxfmr<RemoveOrphanConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_REMOVE_ORPHAN_CONTACT;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<RemoveOrphanConfig>> getSparkJobClz() {
        return RemoveOrphanJob.class;
    }

    @Override
    protected Class<RemoveOrphanConfig> getJobConfigClz() {
        return RemoveOrphanConfig.class;
    }

}
