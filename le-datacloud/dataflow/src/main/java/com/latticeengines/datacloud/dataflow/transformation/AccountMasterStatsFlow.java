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
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsGroupingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsLeafFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsReportFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DimensionExpandFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import cascading.tuple.Fields;

@Component("accountMasterStatsFlow")
public class AccountMasterStatsFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, AccountMasterStatsParameters> {

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {
        DataCloudVersion latestAMVersion = columnMetadataProxy.latestVersion(null);
        List<ColumnMetadata> columnMetadatas = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                latestAMVersion.getVersion());

        Map<String, ColumnMetadata> columnMetadatasMap = new HashMap<>();

        for (ColumnMetadata metadata : columnMetadatas) {
            columnMetadatasMap.put(metadata.getColumnName(), metadata);
        }

        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap = parameters
                .getRequiredDimensionsValuesMap();

        Map<String, Field> allColumns = new LinkedHashMap<>();
        Map<String, Field> minMaxSourceColumns = new LinkedHashMap<>();

        Node node = addSource(parameters.getBaseTables().get(0), allColumns);
        Node minMaxNode = addSource(parameters.getBaseTables().get(1), minMaxSourceColumns);

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

        Fields groupByFields = new Fields();

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];

        node = createDimensionBasedExpandNodes(requiredDimensionsValuesMap, node, dimensionDefinitionMap,
                leafSchemaAllOutputColumns, groupByFields, dimensionIdFieldNames);

        leafSchemaAllOutputColumns.add(new FieldMetadata(getMinMaxKey(), String.class));

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(resultSchema(leafSchemaAllOutputColumns));

        node.renamePipe("leafRecordsNode");

        node = joinWithMinMaxNode(node, minMaxNode, dimensionIdFieldNames);

        fms.add(new FieldMetadata(getTotalKey(), Long.class));
        FieldMetadata minMaxKeyMeta = new FieldMetadata(getMinMaxKey(), String.class);
        fms.add(minMaxKeyMeta);

        node = createGroupingNode(parameters, node, leafSchemaAllOutputColumns, dimensionIdFieldNames, fms);

        fms = node.getSchema();

        node = createReportGenerationNode(parameters, dimensionIdFieldNames, fms, node);

        return node;
    }

    private Node joinWithMinMaxNode(Node node, Node minMaxNode, String[] dimensionIdFieldNames) {
        String[] renamedDimensionFieldNames = new String[dimensionIdFieldNames.length];
        int i = 0;
        for (String id : dimensionIdFieldNames) {
            renamedDimensionFieldNames[i++] = "_RENAMED_" + id;
        }

        node.renamePipe("beginJoinWithMinMax");
        Node joinedWithMinMax = node.join(new FieldList(dimensionIdFieldNames), minMaxNode,
                new FieldList(renamedDimensionFieldNames), JoinType.LEFT);
        joinedWithMinMax.renamePipe("joinedWithMinMax");
        joinedWithMinMax = joinedWithMinMax.discard(new FieldList(renamedDimensionFieldNames));

        return joinedWithMinMax;
    }

    protected Node createLeafGenerationNode(Map<String, ColumnMetadata> columnMetadatasMap,
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node accountMaster,
            Map<String, CategoricalDimension> requiredDimensions, Map<String, List<String>> dimensionDefinitionMap,
            List<FieldMetadata> leafSchemaNewColumns, List<FieldMetadata> inputSchemaDimensionColumns, Fields fields,
            FieldList applyToFieldList, FieldList outputFieldList) {
        AccountMasterStatsLeafFunction.Params functionParams = new AccountMasterStatsLeafFunction.Params(fields,
                leafSchemaNewColumns, inputSchemaDimensionColumns, dimensionDefinitionMap, requiredDimensions,
                columnMetadatasMap, requiredDimensionsValuesMap,
                AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
        AccountMasterStatsLeafFunction leafCreationFunction = new AccountMasterStatsLeafFunction(functionParams);
        accountMaster = accountMaster.apply(leafCreationFunction, applyToFieldList, leafSchemaNewColumns,
                outputFieldList);
        return accountMaster;
    }

    protected Node createDimensionBasedExpandNodes(
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node accountMaster,
            Map<String, List<String>> dimensionDefinitionMap, List<FieldMetadata> finalLeafSchema, Fields groupByFields,
            String[] dimensionIdFieldNames) {
        int i = 0;
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            groupByFields = groupByFields.append(new Fields(dimensionKey));
            dimensionIdFieldNames[i++] = dimensionKey;

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

            FieldList fieldsToApply = new FieldList(allFields);
            FieldList outputFields = new FieldList(allFields);

            DimensionExpandFunction.Params functionParams = new DimensionExpandFunction.Params(expandFields.size(),
                    dimensionKey, new HashMap<String, List<String>>(), expandFields, pos, requiredDimensionsValuesMap,
                    AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
            DimensionExpandFunction dimensionExpandFunction = new DimensionExpandFunction(functionParams);

            accountMaster = accountMaster.apply(dimensionExpandFunction, fieldsToApply, targetField, outputFields,
                    Fields.RESULTS);
        }
        return accountMaster;
    }

    protected Node createGroupingNode(AccountMasterStatsParameters parameters, Node accountMaster,
            List<FieldMetadata> finalLeafSchema, String[] dimensionIdFieldNames, List<FieldMetadata> fms) {
        List<String> attrList = new ArrayList<>();
        List<Integer> attrIdList = new ArrayList<>();

        findAttributeIds(finalLeafSchema, attrList, attrIdList);

        Fields allLeafFields = new Fields();
        for (FieldMetadata fieldMeta : finalLeafSchema) {
            boolean isDimensionField = false;
            for (String id : dimensionIdFieldNames) {
                if (fieldMeta.getFieldName().equals(id)) {
                    isDimensionField = true;
                    break;
                }
            }
            allLeafFields = allLeafFields.append(
                    new Fields(fieldMeta.getFieldName(), isDimensionField ? fieldMeta.getJavaType() : String.class));
        }

        allLeafFields = allLeafFields.append(new Fields(getTotalKey(), Long.class));

        AccountMasterStatsGroupingFunction.Params functionParams = new AccountMasterStatsGroupingFunction.Params(//
                getMinMaxKey(), //
                attrList.toArray(new String[attrList.size()]), //
                attrIdList.toArray(new Integer[attrIdList.size()]), //
                allLeafFields, attrList.toArray(new String[attrList.size()]), getTotalKey(), //
                dimensionIdFieldNames, parameters.getMaxBucketCount(), //
                AccountMasterStatsParameters.LBL_ORDER_POST, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_ENCODED_YES, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_ENCODED_NO, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_NUMERIC, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_BOOLEAN, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_OBJECT, //
                AccountMasterStatsParameters.COUNT_KEY, //
                parameters.getTypeFieldMap(), parameters.getEncodedColumns());
        AccountMasterStatsGroupingFunction buffer = new AccountMasterStatsGroupingFunction(functionParams);

        Node grouped = accountMaster.groupByAndBuffer(new FieldList(dimensionIdFieldNames), //
                buffer, fms);
        grouped = grouped.discard(new FieldList(getMinMaxKey()));
        return grouped;
    }

    protected Node createReportGenerationNode(AccountMasterStatsParameters parameters, String[] dimensionIdFieldNames,
            List<FieldMetadata> fms, Node grouped) {
        List<FieldMetadata> newColumns = getFinalReportColumns(parameters.getFinalDimensionColumns(),
                parameters.getCubeColumnName());

        List<FieldMetadata> fms1 = new ArrayList<>();
        fms1.addAll(newColumns);

        Node report = generateFinalReport(grouped, //
                getFieldList(fms), newColumns, //
                getFieldList(fms1), Fields.RESULTS, parameters.getCubeColumnName(),
                parameters.getRootIdsForNonRequiredDimensions());
        return report;
    }

    private List<FieldMetadata> getFinalReportColumns(List<String> finalDimensionsList, String encodedCubeColumnName) {
        List<FieldMetadata> finalReportColumns = new ArrayList<>();

        for (String dimensionKey : finalDimensionsList) {
            finalReportColumns.add(new FieldMetadata(dimensionKey, Long.class));
        }

        finalReportColumns.add(new FieldMetadata(encodedCubeColumnName, String.class));
        finalReportColumns.add(new FieldMetadata("PID", Long.class));
        return finalReportColumns;
    }

    private Node generateFinalReport(Node grouped, FieldList applyToFieldList, List<FieldMetadata> newColumns,
            FieldList outputFieldList, Fields overrideFieldStrategy, String cubeColumnName,
            Map<String, Long> rootIdsForNonRequiredDimensions) {

        String[] fields = outputFieldList.getFields();

        Fields fieldDeclaration = new Fields(fields);

        AccountMasterStatsReportFunction.Params functionParam = new AccountMasterStatsReportFunction.Params(
                fieldDeclaration, newColumns, cubeColumnName, //
                getTotalKey(), rootIdsForNonRequiredDimensions, //
                AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX, //
                AccountMasterStatsParameters.LBL_ORDER_POST, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_ENCODED_YES, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_ENCODED_NO, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_NUMERIC, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_BOOLEAN, //
                AccountMasterStatsParameters.LBL_ORDER_PRE_OBJECT, //
                AccountMasterStatsParameters.COUNT_KEY);

        AccountMasterStatsReportFunction reportGenerationFunction = new AccountMasterStatsReportFunction(functionParam);
        return grouped.apply(reportGenerationFunction, applyToFieldList, newColumns, outputFieldList,
                overrideFieldStrategy);
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

    private void findAttributeIds(List<FieldMetadata> finalLeafSchema, List<String> attrList,
            List<Integer> attrIdList) {
        int pos = 0;
        for (FieldMetadata field : finalLeafSchema) {
            attrList.add(field.getFieldName());
            attrIdList.add(pos++);
        }
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
        return AccountMasterStatsParameters.GROUP_TOTAL_KEY;
    }

    public String getMinMaxKey() {
        return AccountMasterStatsParameters.MIN_MAX_KEY;
    }
}
