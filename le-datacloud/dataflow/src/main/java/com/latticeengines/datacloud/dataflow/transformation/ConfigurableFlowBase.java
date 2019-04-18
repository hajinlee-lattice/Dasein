package com.latticeengines.datacloud.dataflow.transformation;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public abstract class ConfigurableFlowBase<T extends TransformerConfig>
       extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters>
       implements ConfigurableFlow {

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @SuppressWarnings("unchecked")
    protected T getTransformerConfig(TransformationFlowParameters parameters) {
        T config;

        String confJson = parameters.getConfJson();
        try {
            config = (T)JsonUtils.deserialize(confJson, getTransformerConfigClass());
        } catch (Exception e) {
            config = null;
        }

        return config;
    }

    public abstract Class<? extends TransformerConfig> getTransformerConfigClass();

}
