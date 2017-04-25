package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsLeafFieldSubstitutionFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import cascading.tuple.Fields;

@Component("amStatsLeafSubstitutionFlow")
public class AMStatsLeafSubstitutionFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {
        String dataCloudVersion = parameters.getDataCloudVersion();
        List<ColumnMetadata> columnMetadatas = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                dataCloudVersion);

        Map<String, ColumnMetadata> columnMetadatasMap = new HashMap<>();

        for (ColumnMetadata metadata : columnMetadatas) {
            columnMetadatasMap.put(metadata.getColumnName(), metadata);
        }

        Map<String, Field> allColumns = new LinkedHashMap<>();

        Node node = addSource(parameters.getBaseTables().get(0), allColumns);

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        Fields groupByFields = new Fields();

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];
        List<String> originalDimensionColName = new ArrayList<>();
        int i = 0;
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            groupByFields = groupByFields.append(new Fields(dimensionKey));
            dimensionIdFieldNames[i++] = dimensionKey;
            originalDimensionColName.add(dimensionDefinitionMap.get(dimensionKey).get(0));
        }
        node = node.rename(new FieldList(originalDimensionColName), new FieldList(dimensionIdFieldNames));

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

        if (parameters.isNumericalBucketsRequired()) {
            oldValueColumnsForSubstitution.add(getMinMaxKey());
            newValueColumnsForSubstitution.add(getMinMaxKey());
        }

        node = createLeafFieldSubstitutionNode(node, parameters, //
                inputSchemaForFieldSubstituteFunction, outputSchemaForFieldSubstituteFunction, //
                minMaxAndDimensionList, renamedMinMaxAndDimensionIds, dimensionIdFieldNames, //
                oldValueColumnsForSubstitution, newValueColumnsForSubstitution, renamedNonDimensionFieldIds, //
                parameters.isNumericalBucketsRequired());

        node = node.rename(new FieldList(dimensionIdFieldNames), new FieldList(originalDimensionColName));

        return node;
    }

    private Node createLeafFieldSubstitutionNode(Node node, //
            AccountMasterStatsParameters parameters, //
            List<FieldMetadata> inputSchemaForFieldSubstituteFunction, //
            List<FieldMetadata> outputSchemaForFieldSubstituteFunction, //
            List<FieldMetadata> minMaxAndDimensionList, //
            List<String> renamedMinMaxAndDimensionIds, //
            String[] dimensionIdFieldNames, List<String> oldValueColumnsForSubstitution, //
            List<String> newValueColumnsForSubstitution, List<String> renamedNonDimensionFieldIds, //
            boolean isNumericalBucketsRequired) {

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

        AMStatsLeafFieldSubstitutionFunction.Params functionParams = //
                new AMStatsLeafFieldSubstitutionFunction.Params(//
                        outputFieldsDeclaration, //
                        AccountMasterStatsParameters.ENCODED_YES, //
                        AccountMasterStatsParameters.ENCODED_NO, //
                        TEMP_RENAMED_PREFIX, //
                        getMinMaxKey(), //
                        parameters.getMaxBucketCount(), //
                        parameters.getTypeFieldMap(), parameters.getEncodedColumns(), //
                        parameters.isNumericalBucketsRequired(), minMaxAndDimensionStrList);

        AMStatsLeafFieldSubstitutionFunction leafCreationFunction = //
                new AMStatsLeafFieldSubstitutionFunction(//
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
        if (isNumericalBucketsRequired) {
            node = node.discard(new FieldList(getMinMaxKey()));
        }
        return node;
    }
}
