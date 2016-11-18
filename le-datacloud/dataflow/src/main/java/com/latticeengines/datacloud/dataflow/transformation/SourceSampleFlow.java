package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.SourceSampleFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("sourceSampleFlow")
public class SourceSampleFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, SourceSampleFlowParameters> {

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(SourceSampleFlowParameters parameters) {

        Node source = addSource(parameters.getBaseTables().get(0));

        Node sampled = source.sample(parameters.getFraction());

        String filter = parameters.getFilter();

        List<String> attrs = parameters.getFilterAttrs();

        if ((filter != null) && (attrs != null)) {
            sampled = sampled.filter(filter, new FieldList(attrs.toArray(new String[attrs.size()])));
        }

        return sampled;
    }
}
