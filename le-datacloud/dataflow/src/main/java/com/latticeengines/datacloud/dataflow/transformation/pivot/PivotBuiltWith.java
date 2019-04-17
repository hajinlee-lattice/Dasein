package com.latticeengines.datacloud.dataflow.transformation.pivot;

import static com.latticeengines.datacloud.dataflow.transformation.pivot.PivotBuiltWith.BEAN_NAME;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PivotBuiltWithConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(BEAN_NAME)
public class PivotBuiltWith extends ConfigurableFlowBase<PivotBuiltWithConfig> {

    public static final String BEAN_NAME = "pivotBuiltWith";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PivotBuiltWithConfig config = getTransformerConfig(parameters);
        Node consolidated = addSource(parameters.getBaseTables().get(0));
        return consolidated;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotBuiltWithConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return "pivotBuiltWithTransformer";
    }

}
