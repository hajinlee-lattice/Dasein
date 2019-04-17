package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;

public abstract class TransformationFlowBase<C extends TransformationConfiguration, P extends TransformationFlowParameters>
        extends TypesafeDataFlowBuilder<P> {

    protected C transformationConfiguration;

    protected abstract Class<? extends TransformationConfiguration> getTransConfClass();

    protected String getSourceNameFromTemplate(P parameters, String templateName) {
        String sourceName = null;
        Map<String, String> templateSourceMap = parameters.getTemplateSourceMap();
        if (templateSourceMap != null) {
            sourceName = templateSourceMap.get(templateName);
        }
        return sourceName;
    }
}
