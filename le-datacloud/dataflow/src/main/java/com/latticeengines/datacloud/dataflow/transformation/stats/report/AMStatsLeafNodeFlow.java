package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsLeafFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import cascading.tuple.Fields;

@Component("amStatsLeafNodeFlow")
public class AMStatsLeafNodeFlow extends AMStatsFlowBase {

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

        Map<String, CategoricalDimension> requiredDimensions = parameters.getRequiredDimensions();

        List<FieldMetadata> schema = node.getSchema();

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        List<String> fieldIds = new ArrayList<String>();

        List<List<FieldMetadata>> leafSchema = getLeafSchema(schema, dimensionDefinitionMap, fieldIds);
        List<FieldMetadata> leafSchemaNewColumns = leafSchema.get(0);
        List<FieldMetadata> leafSchemaOldColumns = leafSchema.get(1);
        List<FieldMetadata> inputSchemaDimensionColumns = leafSchema.get(2);
        List<FieldMetadata> leafSchemaAllOutputColumns = new ArrayList<>();
        leafSchemaAllOutputColumns.addAll(leafSchemaOldColumns);
        leafSchemaAllOutputColumns.addAll(leafSchemaNewColumns);

        Fields fields = new Fields(fieldIds.toArray(new String[fieldIds.size()]));

        FieldList applyToFieldList = getFieldList(inputSchemaDimensionColumns);
        FieldList outputFieldList = getFieldList(leafSchemaAllOutputColumns);

        node = createLeafGenerationNode(columnMetadatasMap, requiredDimensionsValuesMap, node, requiredDimensions,
                dimensionDefinitionMap, leafSchemaNewColumns, inputSchemaDimensionColumns, fields, applyToFieldList,
                outputFieldList);

        return node;
    }

    private Node createLeafGenerationNode(Map<String, ColumnMetadata> columnMetadatasMap,
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node accountMaster,
            Map<String, CategoricalDimension> requiredDimensions, Map<String, List<String>> dimensionDefinitionMap,
            List<FieldMetadata> leafSchemaNewColumns, List<FieldMetadata> inputSchemaDimensionColumns, Fields fields,
            FieldList applyToFieldList, FieldList outputFieldList) {
        AMStatsLeafFunction.Params functionParams = new AMStatsLeafFunction.Params(fields,
                inputSchemaDimensionColumns, dimensionDefinitionMap, requiredDimensions, columnMetadatasMap,
                requiredDimensionsValuesMap, AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
        AMStatsLeafFunction leafCreationFunction = new AMStatsLeafFunction(functionParams);
        accountMaster = accountMaster.apply(leafCreationFunction, applyToFieldList, leafSchemaNewColumns,
                outputFieldList);
        return accountMaster;
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
