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

        node = createLeafFieldSubstitutionNode(node, //
                parameters, //
                dimensionIdFieldNames, //
                parameters.isNumericalBucketsRequired());

        node = node.rename(new FieldList(dimensionIdFieldNames), new FieldList(originalDimensionColName));

        return node;
    }

    private Node createLeafFieldSubstitutionNode(Node node, //
            AccountMasterStatsParameters parameters, //
            String[] dimensionIdFieldNames, //
            boolean isNumericalBucketsRequired) {

        List<FieldMetadata> outputSchemaForFieldSubstituteFunction = new ArrayList<>();
        List<String> oldValueColumnsForSubstitution = new ArrayList<>();
        List<String> newValueColumnsForSubstitution = new ArrayList<>();
        List<String> renamedNonDimensionFieldIds = new ArrayList<>();
        List<FieldMetadata> minMaxAndDimensionList = new ArrayList<>();
        List<String> renamedMinMaxAndDimensionIds = new ArrayList<>();
        List<String> combinedRenamedFields = new ArrayList<>();

        List<FieldMetadata> combinedOutputSchemaForFieldSubstituteFunction = processFields(node, parameters,
                dimensionIdFieldNames, outputSchemaForFieldSubstituteFunction, oldValueColumnsForSubstitution,
                newValueColumnsForSubstitution, renamedNonDimensionFieldIds, minMaxAndDimensionList,
                renamedMinMaxAndDimensionIds, combinedRenamedFields);

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
                        TEMP_RENAMED_PREFIX, //
                        getMinMaxKey(), //
                        parameters, minMaxAndDimensionList, node.getFieldNames());

        AMStatsLeafFieldSubstitutionFunction leafCreationFunction = //
                new AMStatsLeafFieldSubstitutionFunction(functionParams);

        node = node.apply(leafCreationFunction, //
                getFieldList(node.getSchema()), //
                outputSchemaForFieldSubstituteFunction, //
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

    private List<FieldMetadata> processFields(Node node, AccountMasterStatsParameters parameters,
            String[] dimensionIdFieldNames, List<FieldMetadata> outputSchemaForFieldSubstituteFunction,
            List<String> oldValueColumnsForSubstitution, List<String> newValueColumnsForSubstitution,
            List<String> renamedNonDimensionFieldIds, List<FieldMetadata> minMaxAndDimensionList,
            List<String> renamedMinMaxAndDimensionIds, List<String> combinedRenamedFields) {
        for (FieldMetadata fieldMeta : node.getSchema()) {
            boolean isDimension = false;
            String fieldName = fieldMeta.getFieldName();
            String renamedFieldName = TEMP_RENAMED_PREFIX + fieldName;

            for (String dimensionFieldName : dimensionIdFieldNames) {
                if (fieldName.equals(dimensionFieldName)) {
                    isDimension = true;
                    break;
                }
            }
            if (fieldName.equals(getMinMaxKey()) || isDimension) {
                minMaxAndDimensionList.add(fieldMeta);
                renamedMinMaxAndDimensionIds.add(renamedFieldName);
            } else {
                FieldMetadata newFieldMeta = new FieldMetadata(renamedFieldName, String.class);
                outputSchemaForFieldSubstituteFunction.add(newFieldMeta);
                oldValueColumnsForSubstitution.add(fieldName);
                newValueColumnsForSubstitution.add(renamedFieldName);
                renamedNonDimensionFieldIds.add(renamedFieldName);

            }
        }

        if (parameters.isNumericalBucketsRequired()) {
            oldValueColumnsForSubstitution.add(getMinMaxKey());
            newValueColumnsForSubstitution.add(getMinMaxKey());
        }

        List<FieldMetadata> combinedOutputSchemaForFieldSubstituteFunction = new ArrayList<>();
        combinedOutputSchemaForFieldSubstituteFunction.addAll(outputSchemaForFieldSubstituteFunction);
        combinedOutputSchemaForFieldSubstituteFunction.addAll(minMaxAndDimensionList);

        combinedRenamedFields.addAll(renamedNonDimensionFieldIds);
        combinedRenamedFields.addAll(renamedMinMaxAndDimensionIds);
        return combinedOutputSchemaForFieldSubstituteFunction;
    }
}
