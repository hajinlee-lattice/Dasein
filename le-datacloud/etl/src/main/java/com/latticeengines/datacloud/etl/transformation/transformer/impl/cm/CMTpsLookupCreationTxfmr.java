package com.latticeengines.datacloud.etl.transformation.transformer.impl.cm;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CREATE_CMTPSLOOKUP;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cm.CMTpsLookupCreationConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cm.CreateCMTpsLookupJob;

@Component(CMTpsLookupCreationTxfmr.TRANSFORMER_NAME)
public class CMTpsLookupCreationTxfmr extends ConfigurableSparkJobTxfmr<CMTpsLookupCreationConfig> {
    public static final String TRANSFORMER_NAME = TRANSFORMER_CREATE_CMTPSLOOKUP;

    @Override
    protected Class<CMTpsLookupCreationConfig> getJobConfigClz() {
        return CMTpsLookupCreationConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<CMTpsLookupCreationConfig>> getSparkJobClz() {
        return CreateCMTpsLookupJob.class;
    }
}
