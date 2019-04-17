package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(Merge.DATAFLOW_BEAN_NAME)
public class Merge extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "MergeFlow";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_MERGE;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        int total = parameters.getBaseTables().size();
        Node base = addSource(parameters.getBaseTables().get(0));
        if (total > 1) {
            List<Node> toMerge = new ArrayList<>();
            for (int i = 1; i < total; i++) {
                toMerge.add(addSource(parameters.getBaseTables().get(i)));
            }
            base = base.merge(toMerge);
        }
        return base;
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
