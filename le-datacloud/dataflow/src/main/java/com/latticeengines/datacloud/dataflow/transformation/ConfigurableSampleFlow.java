package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SampleTransformerConfig;

@Component("configurableSampleFlow")
public class ConfigurableSampleFlow
       extends ConfigurableFlowBase<SampleTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SampleTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        Node sampled = source.sample(config.getFraction());

        String filter = config.getFilter();

        List<String> attrs = config.getFilterAttrs();

        if ((filter != null) && (attrs != null)) {
            sampled = sampled.filter(filter, new FieldList(attrs.toArray(new String[attrs.size()])));
        }

        return sampled;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SampleTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "configurableSampleFlow";
    }

    @Override
    public String getTransformerName() {
        return "sampler";

    }
}
