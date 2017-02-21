package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsLeafFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsMinMaxBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import cascading.tuple.Fields;

@Component("accountMasterStatsMinMaxFlow")
public class AccountMasterStatsMinMaxFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, AccountMasterStatsParameters> {
    private static final String RENAMED_PREFIX = "_RENAMED_";
    private static final String MIN_MAX_JOIN_FIELD = "_JoinFieldMinMax_";
    private static final String MIN_MAX_JOIN_FIELD_RENAMED = RENAMED_PREFIX + "_JoinFieldMinMax_";

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

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

        Set<String> fieldIds = new HashSet<>();

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

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];

        leafSchemaAllOutputColumns.add(new FieldMetadata(getMinMaxKey(), String.class));

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(resultSchema(leafSchemaAllOutputColumns));

        node.renamePipe("leafRecordsNode");
        node = createGroupingAndMinMaxAssessNode(node, leafSchemaAllOutputColumns, dimensionIdFieldNames);

        return node;
    }

    protected Node createLeafGenerationNode(Map<String, ColumnMetadata> columnMetadatasMap,
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node accountMaster,
            Map<String, CategoricalDimension> requiredDimensions, Map<String, List<String>> dimensionDefinitionMap,
            List<FieldMetadata> leafSchemaNewColumns, List<FieldMetadata> inputSchemaDimensionColumns, Fields fields,
            FieldList applyToFieldList, FieldList outputFieldList) {
        AccountMasterStatsLeafFunction.Params functionParams = new AccountMasterStatsLeafFunction.Params(fields,
                inputSchemaDimensionColumns, dimensionDefinitionMap, requiredDimensions,
                columnMetadatasMap, requiredDimensionsValuesMap,
                AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
        AccountMasterStatsLeafFunction leafCreationFunction = new AccountMasterStatsLeafFunction(functionParams);
        accountMaster = accountMaster.apply(leafCreationFunction, applyToFieldList, leafSchemaNewColumns,
                outputFieldList);
        return accountMaster;
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

        AccountMasterStatsMinMaxBuffer.Params functionParams = new AccountMasterStatsMinMaxBuffer.Params(
                minMaxResultFields, getMinMaxKey());
        AccountMasterStatsMinMaxBuffer buffer = new AccountMasterStatsMinMaxBuffer(functionParams);
        node = node.addColumnWithFixedValue(MIN_MAX_JOIN_FIELD, 0, Integer.class);

        fms.add(new FieldMetadata(MIN_MAX_JOIN_FIELD, Integer.class));

        Node minMaxNode = node.groupByAndBuffer(new FieldList(MIN_MAX_JOIN_FIELD), buffer, fms);

        minMaxNode = minMaxNode.rename(new FieldList(MIN_MAX_JOIN_FIELD), new FieldList(MIN_MAX_JOIN_FIELD_RENAMED));

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

    private FieldList getFieldList(List<FieldMetadata> fieldMetadataList) {
        List<String> fields = new ArrayList<>();
        for (FieldMetadata field : fieldMetadataList) {
            fields.add(field.getFieldName());
        }
        FieldList fieldList = new FieldList(fields);
        return fieldList;
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

    public String getTotalKey() {
        return AccountMasterStatsParameters.GROUP_TOTAL_KEY_TEMP;
    }

    public String getMinMaxKey() {
        return AccountMasterStatsParameters.MIN_MAX_KEY;
    }
}
