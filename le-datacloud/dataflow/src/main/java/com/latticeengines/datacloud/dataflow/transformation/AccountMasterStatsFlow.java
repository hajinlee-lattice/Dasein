package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsDimensionAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsDimensionExpandBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsLeafFieldSubstitutionFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsLeafFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterStatsReportFunction;
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

@Component("accountMasterStatsFlow")
public class AccountMasterStatsFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, AccountMasterStatsParameters> {

    private static Log log = LogFactory.getLog(AccountMasterStatsFlow.class);

    private static final String TEMP_RENAMED_PREFIX = "_RENAMED_";
    private static final String MIN_MAX_JOIN_FIELD = "_JoinFieldMinMax_";
    private static final String MIN_MAX_JOIN_FIELD_RENAMED = TEMP_RENAMED_PREFIX + "_JoinFieldMinMax_";

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
        Map<String, Field> minMaxSourceColumns = new LinkedHashMap<>();

        Node node = addSource(parameters.getBaseTables().get(0), allColumns);
        Node minMaxNode = null;
        if (parameters.isNumericalBucketsRequired()) {
            minMaxNode = addSource(parameters.getBaseTables().get(1), minMaxSourceColumns);
        }
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

        Fields groupByFields = new Fields();

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];
        int i = 0;
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            groupByFields = groupByFields.append(new Fields(dimensionKey));
            dimensionIdFieldNames[i++] = dimensionKey;
        }

        if (parameters.isNumericalBucketsRequired()) {
            node = joinWithMinMaxNode(node, minMaxNode, dimensionIdFieldNames);
        }

        List<FieldMetadata> joinedSchema = node.getSchema();

        List<FieldMetadata> inputSchemaForFieldSubstituteFunction = new ArrayList<>();
        List<FieldMetadata> outputSchemaForFieldSubstituteFunction = new ArrayList<>();
        List<String> oldValueColumnsForSubstitution = new ArrayList<>();
        List<String> newValueColumnsForSubstitution = new ArrayList<>();
        List<String> renamedNonDimensionFieldIds = new ArrayList<>();
        List<FieldMetadata> minMaxAndDimensionList = new ArrayList<>();
        List<String> renamedMinMaxAndDimensionIds = new ArrayList<>();

        for (FieldMetadata fieldMeta : joinedSchema) {
            inputSchemaForFieldSubstituteFunction.add(fieldMeta);
            boolean isDimension = false;
            for (String dimensionFieldName : dimensionIdFieldNames) {
                if (fieldMeta.getFieldName().equals(dimensionFieldName)) {
                    isDimension = true;
                    break;
                }
            }
            if (fieldMeta.getFieldName().equals(getMinMaxKey()) || isDimension) {
                minMaxAndDimensionList.add(fieldMeta);
                renamedMinMaxAndDimensionIds.add(TEMP_RENAMED_PREFIX + fieldMeta.getFieldName());
            }

            if (!fieldMeta.getFieldName().equals(getMinMaxKey())) {
                if (!isDimension) {
                    FieldMetadata newFieldMeta = new FieldMetadata(TEMP_RENAMED_PREFIX + fieldMeta.getFieldName(),
                            String.class);
                    outputSchemaForFieldSubstituteFunction.add(newFieldMeta);
                    oldValueColumnsForSubstitution.add(fieldMeta.getFieldName());
                    newValueColumnsForSubstitution.add(TEMP_RENAMED_PREFIX + fieldMeta.getFieldName());
                    renamedNonDimensionFieldIds.add(TEMP_RENAMED_PREFIX + fieldMeta.getFieldName());
                }
            }
        }

        for (String dim : dimensionIdFieldNames) {
            oldValueColumnsForSubstitution.add(dim);
            newValueColumnsForSubstitution.add(dim);
        }

        oldValueColumnsForSubstitution.add(getMinMaxKey());
        newValueColumnsForSubstitution.add(getMinMaxKey());

        node = createLeafFieldSubstitutionNode(node, parameters, //
                inputSchemaForFieldSubstituteFunction, outputSchemaForFieldSubstituteFunction, //
                minMaxAndDimensionList, renamedMinMaxAndDimensionIds, dimensionIdFieldNames, //
                oldValueColumnsForSubstitution, newValueColumnsForSubstitution, renamedNonDimensionFieldIds);

          node = createDimensionBasedAggregateNode(node, dimensionIdFieldNames);

          node = createDimensionBasedExpandAndMergeNodes(requiredDimensionsValuesMap, //
                     node, dimensionDefinitionMap, node.getSchema(), //
                     groupByFields, dimensionIdFieldNames);

          List<FieldMetadata> fms = node.getSchema();
          node = createReportGenerationNode(parameters, dimensionIdFieldNames, fms, node, dataCloudVersion);

        return node;
    }

    private Node createLeafFieldSubstitutionNode(Node node, //
            AccountMasterStatsParameters parameters, //
            List<FieldMetadata> inputSchemaForFieldSubstituteFunction, //
            List<FieldMetadata> outputSchemaForFieldSubstituteFunction, //
            List<FieldMetadata> minMaxAndDimensionList, //
            List<String> renamedMinMaxAndDimensionIds, //
            String[] dimensionIdFieldNames, List<String> oldValueColumnsForSubstitution, //
            List<String> newValueColumnsForSubstitution, List<String> renamedNonDimensionFieldIds) {

        List<FieldMetadata> combinedOutputSchemaForFieldSubstituteFunction = new ArrayList<>();
        combinedOutputSchemaForFieldSubstituteFunction.addAll(outputSchemaForFieldSubstituteFunction);
        combinedOutputSchemaForFieldSubstituteFunction.addAll(minMaxAndDimensionList);

        List<String> combinedRenamedFields = new ArrayList<>();
        combinedRenamedFields.addAll(renamedNonDimensionFieldIds);
        combinedRenamedFields.addAll(renamedMinMaxAndDimensionIds);

        List<String> minMaxAndDimensionStrList = new ArrayList<>();
        for (FieldMetadata fieldMeta : minMaxAndDimensionList) {
            minMaxAndDimensionStrList.add(fieldMeta.getFieldName());
        }

        // it is important to use renamed column names for output schema as we
        // are replacing field values (which can be any type of object) with
        // stats object representation which is in String format.
        //
        // Without using renamed columns, cascading does not allow writing
        // String value for the same fields which may have non-string type value

        Fields outputFieldsDeclaration = new Fields(
                combinedRenamedFields.toArray(new String[combinedRenamedFields.size()]));

        AccountMasterStatsLeafFieldSubstitutionFunction.Params functionParams = //
                new AccountMasterStatsLeafFieldSubstitutionFunction.Params(//
                        outputFieldsDeclaration, //
                        AccountMasterStatsParameters.ENCODED_YES, //
                        AccountMasterStatsParameters.ENCODED_NO, //
                        TEMP_RENAMED_PREFIX, //
                        getMinMaxKey(), //
                        parameters.getMaxBucketCount(), //
                        parameters.getTypeFieldMap(), parameters.getEncodedColumns(), //
                        parameters.isNumericalBucketsRequired(), minMaxAndDimensionStrList);

        AccountMasterStatsLeafFieldSubstitutionFunction leafCreationFunction = //
                new AccountMasterStatsLeafFieldSubstitutionFunction(//
                        functionParams, outputSchemaForFieldSubstituteFunction, //
                        inputSchemaForFieldSubstituteFunction);

        List<FieldMetadata> targetMetadataList = new ArrayList<FieldMetadata>();
        targetMetadataList.addAll(outputSchemaForFieldSubstituteFunction);
        targetMetadataList.addAll(inputSchemaForFieldSubstituteFunction);
        targetMetadataList.addAll(minMaxAndDimensionList);
        node = node.apply(leafCreationFunction, //
                getFieldList(inputSchemaForFieldSubstituteFunction), outputSchemaForFieldSubstituteFunction,
                getFieldList(combinedOutputSchemaForFieldSubstituteFunction));

        // once we have calculated stats obj using renamed columns names, now
        // switch field names back to original names
        node = node.rename(new FieldList(newValueColumnsForSubstitution),
                new FieldList(oldValueColumnsForSubstitution));

        // since we have already made use of minMax info in stats obj
        // calculation, discard this column
        node = node.discard(new FieldList(getMinMaxKey()));
        return node;
    }

    private Node joinWithMinMaxNode(Node node, Node minMaxNode, String[] dimensionIdFieldNames) {

        node.renamePipe("beginJoinWithMinMax");
        node = node.addColumnWithFixedValue(MIN_MAX_JOIN_FIELD, 0, Integer.class);

        Node joinedWithMinMax = node.join(new FieldList(MIN_MAX_JOIN_FIELD), minMaxNode,
                new FieldList(MIN_MAX_JOIN_FIELD_RENAMED), JoinType.INNER);
        joinedWithMinMax.renamePipe("joinedWithMinMax");
        joinedWithMinMax = joinedWithMinMax.discard(new FieldList(MIN_MAX_JOIN_FIELD, MIN_MAX_JOIN_FIELD_RENAMED));

        return joinedWithMinMax;
    }

    private Node createLeafGenerationNode(Map<String, ColumnMetadata> columnMetadatasMap,
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, Node accountMaster,
            Map<String, CategoricalDimension> requiredDimensions, Map<String, List<String>> dimensionDefinitionMap,
            List<FieldMetadata> leafSchemaNewColumns, List<FieldMetadata> inputSchemaDimensionColumns, Fields fields,
            FieldList applyToFieldList, FieldList outputFieldList) {
        AccountMasterStatsLeafFunction.Params functionParams = new AccountMasterStatsLeafFunction.Params(fields,
                inputSchemaDimensionColumns, dimensionDefinitionMap, requiredDimensions, columnMetadatasMap,
                requiredDimensionsValuesMap, AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
        AccountMasterStatsLeafFunction leafCreationFunction = new AccountMasterStatsLeafFunction(functionParams);
        accountMaster = accountMaster.apply(leafCreationFunction, applyToFieldList, leafSchemaNewColumns,
                outputFieldList);
        return accountMaster;
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
            AccountMasterStatsDimensionAggregator aggregator = //
                    new AccountMasterStatsDimensionAggregator(fields);
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

            AccountMasterStatsDimensionExpandBuffer.Params functionParams = //
                    new AccountMasterStatsDimensionExpandBuffer.Params(//
                            dimensionKey, dimensionDefinitionMap, //
                            allLeafFields, pos, requiredDimensionsValuesMap, //
                            AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX);
            AccountMasterStatsDimensionExpandBuffer buffer = //
                    new AccountMasterStatsDimensionExpandBuffer(functionParams);

            node = node.retain(new FieldList(allFields));
            node = node.groupByAndBuffer(new FieldList(groupBy), //
                    buffer, fms);
        }
        return node;
    }

    private Node createReportGenerationNode(AccountMasterStatsParameters parameters, String[] dimensionIdFieldNames,
            List<FieldMetadata> fms, Node grouped, String dataCloudVersion) {
        List<String> splunkReportColumns = new ArrayList<>();
        List<FieldMetadata> reportOutputColumns = getFinalReportColumns(parameters.getFinalDimensionColumns(),
                parameters.getCubeColumnName(), splunkReportColumns);

        List<FieldMetadata> reportOutputColumnsFms = new ArrayList<>();
        reportOutputColumnsFms.addAll(reportOutputColumns);

        Node report = generateFinalReport(grouped, //
                getFieldList(fms), reportOutputColumns, //
                getFieldList(reportOutputColumnsFms), Fields.RESULTS, //
                parameters.getCubeColumnName(), parameters.getRootIdsForNonRequiredDimensions(), //
                splunkReportColumns, parameters.getColumnsForStatsCalculation(), //
                parameters.getColumnIdsForStatsCalculation(), dataCloudVersion);
        return report;
    }

    private List<FieldMetadata> getFinalReportColumns(List<String> finalDimensionsList, String encodedCubeColumnName,
            List<String> splunkReportColumns) {
        List<FieldMetadata> finalReportColumns = new ArrayList<>();

        int index = 0;
        for (String dimensionKey : finalDimensionsList) {
            finalReportColumns.add(new FieldMetadata(dimensionKey, Long.class));
            log.info("Final report column " + index + " " + dimensionKey);
            index++;
        }

        finalReportColumns.add(new FieldMetadata(encodedCubeColumnName, String.class));
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.GROUP_TOTAL_KEY, Long.class));

        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_1_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_1_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_2_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_2_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_3_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_3_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_4_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_4_KEY, String.class));
        return finalReportColumns;
    }

    private Node generateFinalReport(Node grouped, FieldList applyToFieldList, List<FieldMetadata> reportOutputColumns,
            FieldList reportOutputColumnFieldList, Fields overrideFieldStrategy, String cubeColumnName,
            Map<String, Long> rootIdsForNonRequiredDimensions, List<String> splunkReportColumns,
            List<String> columnsForStatsCalculation, List<Integer> columnIdsForStatsCalculation,
            String dataCloudVersion) {

        String[] reportOutputColumnFields = reportOutputColumnFieldList.getFields();

        Fields reportOutputColumnFieldsDeclaration = new Fields(reportOutputColumnFields);

        AccountMasterStatsReportFunction.Params functionParam = //
                new AccountMasterStatsReportFunction.Params(//
                        reportOutputColumnFieldsDeclaration, reportOutputColumns, cubeColumnName, //
                        getTotalKey(), splunkReportColumns, rootIdsForNonRequiredDimensions, //
                        columnsForStatsCalculation, columnIdsForStatsCalculation, //
                        AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX, //
                        AccountMasterStatsParameters.GROUP_TOTAL_KEY);

        AccountMasterStatsReportFunction reportGenerationFunction = new AccountMasterStatsReportFunction(functionParam);
        Node report = grouped.apply(reportGenerationFunction, applyToFieldList, reportOutputColumns,
                reportOutputColumnFieldList, overrideFieldStrategy);

        report = report.addRowID(AccountMasterStatsParameters.PID_KEY);
        report = report.addColumnWithFixedValue(AccountMasterStatsParameters.DATA_CLOUD_VERSION, dataCloudVersion,
                String.class);

        return report;
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

    public String getTotalKey() {
        return AccountMasterStatsParameters.GROUP_TOTAL_KEY_TEMP;
    }

    public String getMinMaxKey() {
        return AccountMasterStatsParameters.MIN_MAX_KEY;
    }
}
