package com.latticeengines.propdata.dataflow.transformation;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class TransformationFlowBase<T extends TransformationConfiguration>
        extends TypesafeDataFlowBuilder<TransformationFlowParameters> {

    protected T transformationConfiguration;

    protected abstract Class<? extends TransformationConfiguration> getTransConfClass();

    protected void readTransformationConfiguration(TransformationFlowParameters parameters) {

    }

}
