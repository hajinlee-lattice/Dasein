package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.BucketedFilter.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETED_FILTER;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.FilterBucketed;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.FilterBucketedParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketedFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(TRANSFORMER_NAME)
public class BucketedFilter extends AbstractDataflowTransformer<BucketedFilterConfig, FilterBucketedParameters> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_BUCKETED_FILTER;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return FilterBucketed.BEAN_NAME;
    }

    @Override
    protected boolean validateConfig(BucketedFilterConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return BucketedFilterConfig.class;
    }

    @Override
    protected Class<FilterBucketedParameters> getDataFlowParametersClass() {
        return FilterBucketedParameters.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, FilterBucketedParameters parameters,
                                         BucketedFilterConfig configuration) {
        parameters.originalAttrs = configuration.getOriginalAttrs();
    }

}
