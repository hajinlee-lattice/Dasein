package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_REPARTITION_TXMFR;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.common.RepartitionConfig;
import com.latticeengines.spark.exposed.job.common.RepartitionJob;


@Component(RepartitionTxfmr.TRANSFORMER_NAME)
public class RepartitionTxfmr extends ConfigurableSparkJobTxfmr<RepartitionConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_REPARTITION_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<RepartitionJob> getSparkJobClz() {
        return RepartitionJob.class;
    }

    @Override
    protected Class<RepartitionConfig> getJobConfigClz() {
        return RepartitionConfig.class;
    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, RepartitionConfig sparkJobConfig,
                                     TransformerConfig configuration, List<Schema> baseSchemas) {
        if (CollectionUtils.isNotEmpty(baseSchemas)) {
            return baseSchemas.get(0);
        } else {
            return null;
        }
    }

}
