package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component(HardDeleteFlow.DATAFLOW_BEAN_NAME)
public class HardDeleteFlow extends ConfigurableFlowBase<CleanupConfig> {
    private static final Logger log = LoggerFactory.getLogger(HardDeleteFlow.class);

    public static final String DATAFLOW_BEAN_NAME = "HardDeleteFlow";
    public static final String TRANSFORMER_NAME = "HardDeleteTransformer";

    private static final String DELETE_PREFIX = "DEL_";

    private CleanupConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        CleanupOperationType type = config.getOperationType();
        BusinessEntity entity = config.getBusinessEntity();
        Node deleteNode = addSource(parameters.getBaseTables().get(0));
        List<String> renamedDeleteSchema = new ArrayList<>();
        deleteNode.getFieldNames().forEach(name -> renamedDeleteSchema.add(DELETE_PREFIX + name));
        deleteNode = deleteNode.rename(new FieldList(deleteNode.getFieldNames()),
                new FieldList(renamedDeleteSchema));
        Node originalNode = addSource(parameters.getBaseTables().get(1));
        List<String> baseColumns = getJoinedColumns(true);
        List<String> deleteColumns = getJoinedColumns(false);
        List<String> fields = originalNode.getFieldNames();
        Node resultNode = null;
        log.info(String.format("Original Node fields: %s", StringUtils.join(originalNode.getFieldNames(), ",")));
        log.info(String.format("Delete Node fields: %s", StringUtils.join(deleteNode.getFieldNames(), ",")));
        if ((entity == BusinessEntity.Account || entity == BusinessEntity.Contact || entity == BusinessEntity.Transaction)) {
            if (type == CleanupOperationType.BYUPLOAD_ID) {
                resultNode = originalNode.leftJoin(baseColumns.get(0), deleteNode, deleteColumns.get(0))
                        .filter(deleteColumns.get(0) + " == null", new FieldList(deleteColumns.get(0)))
                        .retain(new FieldList(fields));
            } else {
                throw new RuntimeException("Account/Contact/Transaction hard delete does not support type: " + type.name());
            }
        } else {
            throw new RuntimeException("Not supported BusinessEntity type: " + entity.name());
        }
        return resultNode;
    }


    private List<String> getJoinedColumns(boolean isBase) {
        List<String> result = new ArrayList<>();
        CleanupConfig.JoinedColumns joinedColumns = isBase ? config.getBaseJoinedColumns() : config.getDeleteJoinedColumns();
        String prefix = isBase ? "" : DELETE_PREFIX;
        result.add(prefix + joinedColumns.getAccountId());
        return result;
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
