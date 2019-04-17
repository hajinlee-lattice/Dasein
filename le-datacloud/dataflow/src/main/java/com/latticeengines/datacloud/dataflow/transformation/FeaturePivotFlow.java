package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(FeaturePivotFlow.DATAFLOW_BEAN_NAME)
public class FeaturePivotFlow extends PivotFlow {
    public final static String DATAFLOW_BEAN_NAME = "featurePivotFlowTransform";
    public final static String TRANSFORMER_NAME = "featurePivotFlowTransformer";

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = super.construct(parameters);
        return node.filter("Execution_Full == null || Execution_Full > 0", new FieldList("Execution_Full"));
    }
}
