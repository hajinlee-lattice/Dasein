package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component(CleanupFlow.DATAFLOW_BEAN_NAME)
public class CleanupFlow extends ConfigurableFlowBase<CleanupConfig> {

    private static final Logger log = LoggerFactory.getLogger(CleanupFlow.class);

    public static final String DATAFLOW_BEAN_NAME = "CleanupFlow";
    public static final String TRANSFORMER_NAME = "CleanupTransformer";

    private static final String DELETE_PREFIX = "DELETE_";
    private static final String AGGREGATE_PREFIX = "AGGR_";

    private static final String DUMMY_COLUMN = "DeleteDummyColumn_f8f7d5c7_ddfe_4fd8_bd51_876be6f3f7bc";

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
        List<String> baseColumns = getJoinedColumns(entity, type, true);
        List<String> deleteColumns = getJoinedColumns(entity, type, false);
        List<String> fields = originalNode.getFieldNames();
        Node resultNode = null;
        log.info(String.format("Original Node fields: %s", StringUtils.join(originalNode.getFieldNames(), ",")));
        log.info(String.format("Delete Node fields: %s", StringUtils.join(deleteNode.getFieldNames(), ",")));
        if ((entity == BusinessEntity.Account || entity == BusinessEntity.Contact)) {
            if (type == CleanupOperationType.BYUPLOAD_ID) {
                resultNode = originalNode.leftJoin(baseColumns.get(0), deleteNode, deleteColumns.get(0))
                        .filter(deleteColumns.get(0) + " == null", new FieldList(deleteColumns.get(0)))
                        .retain(new FieldList(fields));
            } else {
                throw new RuntimeException("Account/Contact Cleanup does not support type: " + type.name());
            }
        } else if (entity == BusinessEntity.Transaction) {
            switch (type) {
                case BYUPLOAD_ACPD:
                    FieldList lfl = new FieldList(baseColumns.get(0), baseColumns.get(2), baseColumns.get(3));
                    FieldList rfl = new FieldList(deleteColumns.get(0), deleteColumns.get(2), deleteColumns.get(3));
                    resultNode = originalNode.leftJoin(lfl, deleteNode, rfl);
                    Node partA = resultNode
                            .filter(deleteColumns.get(0) + " == null", new FieldList(deleteColumns.get(0)))
                            .retain(new FieldList(fields));
                    Node partC = resultNode
                            .filter(deleteColumns.get(1) + " != null && " +
                                deleteColumns.get(1) + " != "  + baseColumns.get(1),
                                new FieldList(deleteColumns.get(1), baseColumns.get(1)))
                            .retain(new FieldList(fields));
                    resultNode = partA.merge(partC);
                    break;
                case BYUPLOAD_MINDATE:
                    log.info(String.format("Delete column name: %s, avro type: %s, java type: %s", deleteColumns.get(0),
                            deleteNode.getSchema(deleteColumns.get(0)).getAvroType().getName(),
                            deleteNode.getSchema(deleteColumns.get(0)).getJavaType().getName()));
                    log.info(String.format("Base column name: %s, avro type: %s, java type: %s", baseColumns.get(0),
                            originalNode.getSchema(baseColumns.get(0)).getAvroType().getName(),
                            originalNode.getSchema(baseColumns.get(0)).getJavaType().getName()));
                    deleteNode = deleteNode
                            .filter(deleteColumns.get(0) + " != null ", new FieldList(deleteColumns.get(0)))
                            .sort(deleteColumns.get(0)).limit(1);
                    originalNode = originalNode.addColumnWithFixedValue(DUMMY_COLUMN, "dummyId", String.class);
                    deleteNode = deleteNode.addColumnWithFixedValue(DELETE_PREFIX + DUMMY_COLUMN,
                            "dummyId", String.class);
                    originalNode = originalNode.leftJoin(DUMMY_COLUMN, deleteNode, DELETE_PREFIX + DUMMY_COLUMN);
//                    deleteNode = deleteNode.leftJoin(DUMMY_COLUMN, originalNode, DUMMY_COLUMN);
                    resultNode = originalNode
                            .filter( baseColumns.get(0) + " < " + deleteColumns.get(0),
                                    new FieldList(baseColumns.get(0), deleteColumns.get(0)))
                            .retain(new FieldList(fields));
                    break;
                case BYUPLOAD_MINDATEANDACCOUNT:
                    Aggregation aggregation = new Aggregation(deleteColumns.get(1),
                            AGGREGATE_PREFIX + deleteColumns.get(1), AggregationType.MIN);
                    deleteNode = deleteNode
                            .filter(deleteColumns.get(1) + " > 0", new FieldList(deleteColumns.get(1)));
                    deleteNode = deleteNode.groupBy(new FieldList(deleteColumns.get(0)), Arrays.asList(aggregation));
                    Node part1 = originalNode.leftJoin(baseColumns.get(0), deleteNode, deleteColumns.get(0))
                            .filter(deleteColumns.get(0) + " == null", new FieldList(deleteColumns.get(0)))
                            .retain(new FieldList(fields));
                    Node part2 = originalNode.innerJoin(baseColumns.get(0), deleteNode, deleteColumns.get(0))
                            .filter(baseColumns.get(1) + " < " + AGGREGATE_PREFIX + deleteColumns.get(1),
                                    new FieldList(baseColumns.get(1), AGGREGATE_PREFIX + deleteColumns.get(1)))
                            .retain(new FieldList(fields));
                    resultNode = part1.merge(part2);
                    break;
                default:
                    throw new RuntimeException("Transaction Cleanup does not support type: " + type.name());
            }
        } else {
            throw new RuntimeException("Not supported BusinessEntity type: " + entity.name());
        }
        return resultNode;
    }


    private List<String> getJoinedColumns(BusinessEntity businessEntity, CleanupOperationType type, boolean isBase) {
        List<String> result = new ArrayList<>();
        CleanupConfig.JoinedColumns joinedColumns = isBase ? config.getBaseJoinedColumns() : config.getDeleteJoinedColumns();
        String prefix = isBase ? "" : DELETE_PREFIX;
        switch (businessEntity) {
            case Account:
                result.add(prefix + joinedColumns.getAccountId());
                break;
            case Contact:
                result.add(prefix + joinedColumns.getContactId());
                break;
            case Transaction:
                switch (type) {
                    case BYUPLOAD_ACPD:
                        result.add(prefix + joinedColumns.getAccountId());
                        result.add(prefix + joinedColumns.getContactId());
                        result.add(prefix + joinedColumns.getProductId());
                        result.add(prefix + joinedColumns.getTransactionTime());
                        break;
                    case BYUPLOAD_MINDATE:
                        result.add(prefix + joinedColumns.getTransactionTime());
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        result.add(prefix + joinedColumns.getAccountId());
                        result.add(prefix + joinedColumns.getTransactionTime());
                        break;
                    default:
                        throw new RuntimeException("Transaction Cleanup does not suport type: " + type.name());
                }
        }
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
