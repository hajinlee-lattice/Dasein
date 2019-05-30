package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;

@Component(MergeImportsTxfmr.TRANSFORMER_NAME)
public class MergeImportsTxfmr extends ConfigurableSparkJobTxfmr<MergeImportsConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_MERGE_IMPORTS;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<MergeImportsJob> getSparkJobClz() {
        return MergeImportsJob.class;
    }

    @Override
    protected Class<MergeImportsConfig> getJobConfigClz() {
        return MergeImportsConfig.class;
    }

}
