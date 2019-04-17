package com.latticeengines.datacloud.dataflow.transformation.ams;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.am.AccountMasterBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AMSeedCleanup.DATAFLOW_BEAN_NAME)
public class AMSeedCleanup extends AccountMasterBase<AMSeedMarkerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedCleanup";
    public static final String TRANSFORMER_NAME = "AMSeedCleanupTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(FLAG_DROP_OOB_ENTRY);
        node = node.filter(FLAG_DROP_OOB_ENTRY + " == 0", fieldList) //
                .discard(fieldList);
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
