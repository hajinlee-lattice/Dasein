package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(AMSeedCleanup.DATAFLOW_BEAN_NAME)
public class AMSeedCleanup extends AccountMasterBase<AMSeedMarkerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedCleanup";
    public static final String TRANSFORMER_NAME = "AMSeedCleanupTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(FLAG_DROP_OOB_ENTRY, FLAG_DROP_SMALL_BUSINESS, FLAG_DROP_INCORRECT_DATA,
                FLAG_DROP_LESS_POPULAR_DOMAIN, FLAG_DROP_ORPHAN_ENTRY);

        node = node.filter(FLAG_DROP_OOB_ENTRY + " == 0 && " + FLAG_DROP_SMALL_BUSINESS + " == 0 && "
                + FLAG_DROP_INCORRECT_DATA + " == 0 && " + FLAG_DROP_ORPHAN_ENTRY + " == 0 ", fieldList);

        node = node.discard(fieldList);
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
