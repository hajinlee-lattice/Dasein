package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsMinMaxBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsMinMaxFlow")
public class AMStatsMinMaxFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        List<FieldMetadata> schema = node.getSchema();

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        Set<String> fieldIds = new HashSet<>();

        List<List<FieldMetadata>> leafSchema = getLeafSchema(schema, dimensionDefinitionMap, fieldIds);
        List<FieldMetadata> leafSchemaNewColumns = leafSchema.get(0);
        List<FieldMetadata> leafSchemaOldColumns = leafSchema.get(1);
        List<FieldMetadata> leafSchemaAllOutputColumns = new ArrayList<>();
        leafSchemaAllOutputColumns.addAll(leafSchemaOldColumns);
        leafSchemaAllOutputColumns.addAll(leafSchemaNewColumns);

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];

        leafSchemaAllOutputColumns.add(new FieldMetadata(getMinMaxKey(), String.class));

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(resultSchema(leafSchemaAllOutputColumns));

        node.renamePipe("leafRecordsNode");
        node = createGroupingAndMinMaxAssessNode(node, leafSchemaAllOutputColumns, dimensionIdFieldNames);

        return node;
    }

    private Node createGroupingAndMinMaxAssessNode(Node node, List<FieldMetadata> finalLeafSchema,
            String[] dimensionIdFieldNames) {
        Fields minMaxResultFields = new Fields();

        List<FieldMetadata> fms = new ArrayList<>();

        for (FieldMetadata fieldMeta : finalLeafSchema) {
            boolean shouldRetain = false;
            for (String dimensionId : dimensionIdFieldNames) {
                if (fieldMeta.getFieldName().equals(dimensionId)) {
                    shouldRetain = false;
                    break;
                }
            }

            if (!shouldRetain) {
                if (fieldMeta.getFieldName().equals(getMinMaxKey())) {
                    shouldRetain = true;
                }
            }

            if (shouldRetain) {
                minMaxResultFields = minMaxResultFields
                        .append(new Fields(fieldMeta.getFieldName(), fieldMeta.getJavaType()));
                fms.add(fieldMeta);
            }
        }

        minMaxResultFields = minMaxResultFields.append(new Fields(MIN_MAX_JOIN_FIELD, Integer.class));

        AMStatsMinMaxBuffer.Params functionParams = new AMStatsMinMaxBuffer.Params(
                minMaxResultFields, getMinMaxKey());
        AMStatsMinMaxBuffer buffer = new AMStatsMinMaxBuffer(functionParams);
        node = node.addColumnWithFixedValue(MIN_MAX_JOIN_FIELD, 0, Integer.class);

        fms.add(new FieldMetadata(MIN_MAX_JOIN_FIELD, Integer.class));

        Node minMaxNode = node.groupByAndBuffer(new FieldList(MIN_MAX_JOIN_FIELD), buffer, fms);

        return minMaxNode;
    }

    private List<FieldMetadata> resultSchema(List<FieldMetadata> leafSchemaAllOutputColumns) {
        List<FieldMetadata> resultSchema = new ArrayList<>();

        for (FieldMetadata metadata : leafSchemaAllOutputColumns) {
            resultSchema.add(new FieldMetadata(metadata.getFieldName(),
                    metadata.getFieldName().startsWith(AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX)
                            ? String.class : String.class));
        }

        return resultSchema;
    }

    private List<List<FieldMetadata>> getLeafSchema(List<FieldMetadata> schema,
            Map<String, List<String>> dimensionDefinitionMap, Set<String> fieldIds) {
        List<FieldMetadata> leafSchemaOldColumns = new ArrayList<>();
        List<FieldMetadata> leafSchemaNewColumns = new ArrayList<>();
        List<FieldMetadata> inputSchemaDimensionColumns = new ArrayList<>();
        Map<String, String> subDimensionMap = new HashMap<>();

        for (String key : dimensionDefinitionMap.keySet()) {
            List<String> dimensionSubList = dimensionDefinitionMap.get(key);
            for (String subDimension : dimensionSubList) {
                subDimensionMap.put(subDimension, key);
            }
        }

        Map<String, Integer> tempTrackingMap = new HashMap<>();

        for (FieldMetadata field : schema) {
            String fieldName = field.getFieldName();
            if (subDimensionMap.containsKey(fieldName)) {
                inputSchemaDimensionColumns.add(field);

                String dimensionId = subDimensionMap.get(fieldName);

                if (tempTrackingMap.containsKey(dimensionId)) {
                    continue;
                }

                tempTrackingMap.put(dimensionId, 1);
                FieldMetadata dimensionIdSchema = new FieldMetadata(//
                        Schema.Type.LONG, Long.class, field.getFieldName(), //
                        field.getField(), field.getProperties(), null);
                dimensionIdSchema.setFieldName(dimensionId);
                leafSchemaNewColumns.add(dimensionIdSchema);
                fieldIds.add(dimensionId);
                continue;
            }
            leafSchemaOldColumns.add(field);
        }

        List<List<FieldMetadata>> leafSchema = new ArrayList<>();
        leafSchema.add(leafSchemaNewColumns);
        leafSchema.add(leafSchemaOldColumns);
        leafSchema.add(inputSchemaDimensionColumns);
        return leafSchema;
    }
}
