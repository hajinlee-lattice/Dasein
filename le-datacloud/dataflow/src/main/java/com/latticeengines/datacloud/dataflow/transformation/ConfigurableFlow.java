package com.latticeengines.datacloud.dataflow.transformation;

import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public interface ConfigurableFlow {

    Class<? extends TransformerConfig> getTransformerConfigClass();

    String getDataFlowBeanName();

    String getTransformerName();
}
