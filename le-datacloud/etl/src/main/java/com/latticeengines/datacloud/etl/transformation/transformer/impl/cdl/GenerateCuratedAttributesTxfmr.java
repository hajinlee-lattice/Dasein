package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_GENERATE_CURATED_ATTRIBUTES;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateCuratedAttributes;

@Component(GenerateCuratedAttributesTxfmr.TRANSFORMER_NAME)
public class GenerateCuratedAttributesTxfmr extends ConfigurableSparkJobTxfmr<GenerateCuratedAttributesConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_GENERATE_CURATED_ATTRIBUTES;

    @Override
    protected Class<GenerateCuratedAttributesConfig> getJobConfigClz() {
        return GenerateCuratedAttributesConfig.class;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends AbstractSparkJob<GenerateCuratedAttributesConfig>> getSparkJobClz() {
        return GenerateCuratedAttributes.class;
    }
}
