package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(AMSeedJunkyard.DATAFLOW_BEAN_NAME)
public class AMSeedJunkyard extends AccountMasterBase<AMSeedMarkerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedJunkyard";
    public static final String TRANSFORMER_NAME = "AMSeedJunkyardTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(FLAG_DROP_OOB_ENTRY, FLAG_DROP_SMALL_BUSINESS, FLAG_DROP_INCORRECT_DATA,
                FLAG_DROP_ORPHAN_ENTRY);

        node = node.filter(FLAG_DROP_OOB_ENTRY + " == 1 || " + FLAG_DROP_SMALL_BUSINESS + " == 1 || "
                + FLAG_DROP_INCORRECT_DATA + " == 1 || " + FLAG_DROP_ORPHAN_ENTRY + " == 1 ", fieldList);

        return node;
    }
    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AMSeedMarkerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;

    }
}
