package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component(CleanupFlow.DATAFLOW_BEAN_NAME)
public class CleanupFlow extends ConfigurableFlowBase<CleanupConfig> {

    public static final String DATAFLOW_BEAN_NAME = "CleanupFlow";
    public static final String TRANSFORMER_NAME = "CleanupTransformer";

    private CleanupConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        CleanupOperationType type = config.getOperationType();
        BusinessEntity entity = config.getBusinessEntity();
        Node node3 = null;
        if ((entity == BusinessEntity.Account || entity == BusinessEntity.Contact)
                && type == CleanupOperationType.BYUPLOAD_ID) {
            Node originalNode = addSource(parameters.getBaseTables().get(0));
            Node deleteNode = addSource(parameters.getBaseTables().get(1));
            String originalNodeColumn = config.getJoinColumn();
            String deleteNodeColumn = deleteNode.getFieldNames().get(0);
            List<String> renamedDeleteSchema = new ArrayList<>();
            deleteNode.getFieldNames().forEach(name -> renamedDeleteSchema.add("DEL_" + name));
            deleteNode = deleteNode.rename(new FieldList(deleteNode.getFieldNames()),
                    new FieldList(renamedDeleteSchema));
            List<String> fields = originalNode.getFieldNames();
            String constructedName = "DEL_" + deleteNodeColumn;
            node3 = originalNode.leftJoin(originalNodeColumn, deleteNode, constructedName)
                    .filter(constructedName + " == null", new FieldList(constructedName)).retain(new FieldList(fields));
        }
        return node3;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return CleanupConfig.class;
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
