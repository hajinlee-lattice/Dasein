package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.SelectByColumnTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SELECT_BY_COLUMN_TXFMR;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.SelectByColumnJob;

@Component(TRANSFORMER_NAME)
public class SelectByColumnTxfmr extends ConfigurableSparkJobTxfmr<SelectByColumnConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_SELECT_BY_COLUMN_TXFMR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<SelectByColumnConfig>> getSparkJobClz() {
        return SelectByColumnJob.class;
    }

    @Override
    protected Class<SelectByColumnConfig> getJobConfigClz() {
        return SelectByColumnConfig.class;
    }
}
