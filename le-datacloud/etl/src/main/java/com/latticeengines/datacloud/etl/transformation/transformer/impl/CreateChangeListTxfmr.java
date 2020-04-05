package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CHANGELIST_TXMFR;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.spark.exposed.job.cdl.CreateChangeListJob;

@Component(CreateChangeListTxfmr.TRANSFORMER_NAME)
public class CreateChangeListTxfmr extends ConfigurableSparkJobTxfmr<ChangeListConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_CHANGELIST_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<CreateChangeListJob> getSparkJobClz() {
        return CreateChangeListJob.class;
    }

    @Override
    protected Class<ChangeListConfig> getJobConfigClz() {
        return ChangeListConfig.class;
    }

}
