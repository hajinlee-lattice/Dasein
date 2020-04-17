package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_REPORT_CHANGELIST_TXMFR;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.spark.exposed.job.cdl.ReportChangeListJob;

@Component(ReportChangeListTxfmr.TRANSFORMER_NAME)
public class ReportChangeListTxfmr extends ConfigurableSparkJobTxfmr<ChangeListConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_REPORT_CHANGELIST_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<ReportChangeListJob> getSparkJobClz() {
        return ReportChangeListJob.class;
    }

    @Override
    protected Class<ChangeListConfig> getJobConfigClz() {
        return ChangeListConfig.class;
    }

}
