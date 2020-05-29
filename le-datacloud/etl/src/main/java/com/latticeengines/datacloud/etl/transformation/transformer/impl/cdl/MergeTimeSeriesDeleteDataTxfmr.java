package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_TS_DELETE_TXFMR;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MergeTimeSeriesDeleteData;

@Component(MergeTimeSeriesDeleteDataTxfmr.TRANSFORMER_NAME)
public class MergeTimeSeriesDeleteDataTxfmr extends ConfigurableSparkJobTxfmr<MergeTimeSeriesDeleteDataConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_MERGE_TS_DELETE_TXFMR;

    @Override
    protected Class<MergeTimeSeriesDeleteDataConfig> getJobConfigClz() {
        return MergeTimeSeriesDeleteDataConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<MergeTimeSeriesDeleteDataConfig>> getSparkJobClz() {
        return MergeTimeSeriesDeleteData.class;
    }
}
