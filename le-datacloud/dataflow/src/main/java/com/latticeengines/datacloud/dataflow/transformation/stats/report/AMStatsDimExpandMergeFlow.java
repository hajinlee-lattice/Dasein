package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionExpandBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import cascading.tuple.Fields;

@Component("amStatsDimExpandMergeFlow")
public class AMStatsDimExpandMergeFlow extends AMStatsFlowBase {

    private static Log log = LogFactory.getLog(AMStatsDimExpandMergeFlow.class);

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {
        String dataCloudVersion = parameters.getDataCloudVersion();
        List<ColumnMetadata> columnMetadatas = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                dataCloudVersion);

        Map<String, ColumnMetadata> columnMetadatasMap = new HashMap<>();

        for (ColumnMetadata metadata : columnMetadatas) {
            columnMetadatasMap.put(metadata.getColumnName(), metadata);
        }

        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap = parameters
                .getRequiredDimensionsValuesMap();

        Map<String, Field> allColumns = new LinkedHashMap<>();

        Node node = addSource(parameters.getBaseTables().get(0), allColumns);

        List<FieldMetadata> schema = node.getSchema();

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        List<String> fieldIds = new ArrayList<String>();

        List<List<FieldMetadata>> leafSchema = getLeafSchema(schema, dimensionDefinitionMap, fieldIds);
        List<FieldMetadata> leafSchemaNewColumns = leafSchema.get(0);
        List<FieldMetadata> leafSchemaOldColumns = leafSchema.get(1);
        List<FieldMetadata> leafSchemaAllOutputColumns = new ArrayList<>();
        leafSchemaAllOutputColumns.addAll(leafSchemaOldColumns);
        leafSchemaAllOutputColumns.addAll(leafSchemaNewColumns);

        Fields groupByFields = new Fields();

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];
        int i = 0;
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            groupByFields = groupByFields.append(new Fields(dimensionKey));
            dimensionIdFieldNames[i++] = dimensionKey;
        }

        node = createDimensionBasedAggregateNode(node, dimensionIdFieldNames);

        node = createDimensionBasedExpandAndMergeNodes(requiredDimensionsValuesMap, //
                node, dimensionDefinitionMap, node.getSchema(), //
                groupByFields, dimensionIdFieldNames);

        return node;
    }

    private Node createDimensionBasedAggregateNode(Node node, String[] dimensionIdFieldNames) {

        Fields fields = new Fields();
        List<String> groupBy = new ArrayList<>();
        int idx = 0;
        String[] allFields = new String[node.getSchema().size()];
        for (FieldMetadata fieldMeta : node.getSchema()) {
            String name = fieldMeta.getFieldName();
            if (name.equals("_Location_") || name.equals("_Industry_")) {
                log.info("DimensionName " + name + " Position " + idx);
            }
            allFields[idx++] = name;
            fields = fields.append(new Fields(name, fieldMeta.getJavaType()));
            for (String dimensionId : dimensionIdFieldNames) {
                if (name.equals(dimensionId)) {
                    groupBy.add(name);
                    break;
                }
            }
        }
        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(node.getSchema());

        node = node.retain(new FieldList(allFields));
        AMStatsDimensionAggregator aggregator = //
                new AMStatsDimensionAggregator(fields);
        node = node.groupByAndAggregate(new FieldList(groupBy), aggregator, fms);

        return node;
    }

    private Node createDimensionBasedExpandAndMergeNodes(
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node node,
            Map<String, List<String>> dimensionDefinitionMap, List<FieldMetadata> finalLeafSchema, Fields groupByFields,
            String[] dimensionIdFieldNames) {
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            List<String> groupBy = new ArrayList<>();
            for (String dimensionIdFieldName : dimensionDefinitionMap.keySet()) {
                if (!dimensionIdFieldName.equals(dimensionKey)) {
                    groupBy.add(dimensionIdFieldName);
                }
            }

            Fields expandFields = new Fields();
            List<FieldMetadata> targetField = new ArrayList<>();
            String[] allFields = new String[finalLeafSchema.size()];

            int pos = 0;
            int idx = 0;
            for (FieldMetadata s : finalLeafSchema) {
                expandFields = expandFields.append(new Fields(s.getFieldName()));
                if (s.getFieldName().equals(dimensionKey)) {
                    pos = idx;
                }
                allFields[idx++] = s.getFieldName();
                targetField.add(s);
            }

            List<String> attrList = new ArrayList<>();
            List<Integer> attrIdList = new ArrayList<>();

            findAttributeIds(finalLeafSchema, attrList, attrIdList);

            Fields allLeafFields = new Fields();
            for (FieldMetadata fieldMeta : finalLeafSchema) {

                allLeafFields = allLeafFields.append(new Fields(fieldMeta.getFieldName(), fieldMeta.getJavaType()));
            }

            List<FieldMetadata> fms = new ArrayList<>();
            fms.addAll(node.getSchema());

            AMStatsDimensionExpandBuffer.Params functionParams = //
                    new AMStatsDimensionExpandBuffer.Params(//
                            dimensionKey, dimensionDefinitionMap, //
                            allLeafFields, pos, requiredDimensionsValuesMap, //
                            AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
            AMStatsDimensionExpandBuffer buffer = //
                    new AMStatsDimensionExpandBuffer(functionParams);

            node = node.retain(new FieldList(allFields));
            node = node.groupByAndBuffer(new FieldList(groupBy), //
                    buffer, fms);
        }
        return node;
    }

    private void findAttributeIds(List<FieldMetadata> finalLeafSchema, List<String> attrList,
            List<Integer> attrIdList) {
        int pos = 0;
        for (FieldMetadata field : finalLeafSchema) {
            attrList.add(field.getFieldName());
            attrIdList.add(pos++);
        }
    }

    private List<List<FieldMetadata>> getLeafSchema(List<FieldMetadata> schema,
            Map<String, List<String>> dimensionDefinitionMap, List<String> fieldIds) {
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
