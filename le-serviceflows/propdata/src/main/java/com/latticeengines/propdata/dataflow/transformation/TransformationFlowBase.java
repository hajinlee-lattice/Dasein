package com.latticeengines.propdata.dataflow.transformation;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class TransformationFlowBase<C extends TransformationConfiguration, P extends TransformationFlowParameters>
        extends TypesafeDataFlowBuilder<P> {

    protected C transformationConfiguration;

    protected abstract Class<? extends TransformationConfiguration> getTransConfClass();

}
