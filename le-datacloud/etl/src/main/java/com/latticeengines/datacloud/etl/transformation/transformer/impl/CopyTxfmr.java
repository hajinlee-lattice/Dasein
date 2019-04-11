package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXMFR;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.spark.exposed.job.common.CopyJob;


@Component(CopyTxfmr.TRANSFORMER_NAME)
public class CopyTxfmr extends ConfigurableSparkJobTxfmr<CopyConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_COPY_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<CopyJob> getSparkJobClz() {
        return CopyJob.class;
    }

    @Override
    protected Class<CopyConfig> getJobConfigClz() {
        return CopyConfig.class;
    }

    @Override
    protected String getSecondaryJobName(TransformStep step) {
        Source base = step.getBaseSources()[0];
        if (base instanceof TableSource) {
            TableSource tableSource = (TableSource) base;
            return tableSource.getTable().getName();
        } else {
            return base.getSourceName();
        }
    }

}
