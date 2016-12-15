package com.latticeengines.datacloud.dataflow.transformation;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public abstract class ConfigurableFlowBase<T extends TransformerConfig>
       extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters>
       implements ConfigurableFlow {

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @SuppressWarnings("unchecked")
    protected T getTransformerConfig(TransformationFlowParameters parameters) {
        T config = null;

        String confJson = parameters.getConfJson();
        try {
            config = (T)JsonUtils.deserialize(confJson, getTransformerConfigClass());
        } catch (Exception e) {
            config = null;
        }

        return config;
    }

    abstract public Class<? extends TransformerConfig> getTransformerConfigClass();

}
