package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;


@Component(Join.DATAFLOW_BEAN_NAME)
public class Join extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "JoinFlow";
    public static final String TRANSFORMER_NAME = "JoinTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node src1 = addSource(parameters.getBaseTables().get(0));
        Node src2 = addSource(parameters.getBaseTables().get(1));
        Node joined = src1.coGroup(new FieldList("LatticeID"), Arrays.asList(src2),
                Arrays.asList(new FieldList("LatticeID")), JoinType.INNER);
        return joined;
    }

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
        return TransformerConfig.class;
    }
}
