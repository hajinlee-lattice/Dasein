package com.latticeengines.datacloud.etl.transformation.transformer.impl.cm;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CREATE_CMTPSSOURCE;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cm.CreateCMTpsSourceJob;

@Component(CMTpsSourceCreationTxfmr.TRANSFORMER_NAME)
public class CMTpsSourceCreationTxfmr extends ConfigurableSparkJobTxfmr<CMTpsSourceCreationConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_CREATE_CMTPSSOURCE;

    @Override
    protected Class<CMTpsSourceCreationConfig> getJobConfigClz() {
        return CMTpsSourceCreationConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<CMTpsSourceCreationConfig>> getSparkJobClz() {
        return CreateCMTpsSourceJob.class;
    }
}
