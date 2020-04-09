package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_CURATED_ATTRIBUTES;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.MergeCuratedAttributesConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MergeCuratedAttributes;

@Component(MergeCuratedAttributesTxfmr.TRANSFORMER_NAME)
public class MergeCuratedAttributesTxfmr extends ConfigurableSparkJobTxfmr<MergeCuratedAttributesConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_MERGE_CURATED_ATTRIBUTES;

    @Override
    protected Class<MergeCuratedAttributesConfig> getJobConfigClz() {
        return MergeCuratedAttributesConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<MergeCuratedAttributesConfig>> getSparkJobClz() {
        return MergeCuratedAttributes.class;
    }
}
