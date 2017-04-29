package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsLeafFieldSubstitutionFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsLeafSubstitutionFlow")
public class AMStatsLeafSubstitutionFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<Object>> minMaxInfo = null;
        if (parameters.isNumericalBucketsRequired()) {
            String minMaxData = readMinMaxInfo(parameters.getBaseTables().get(1));
            minMaxInfo = parseMinMaxInfo(minMaxData);
        }

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
                parameters.isNumericalBucketsRequired(), //
                minMaxInfo);

        return node.rename(new FieldList(dimensionIdFieldNames), //
                new FieldList(originalDimensionColName));
    }

    private String readMinMaxInfo(String minMaxDataSourceName) {
        List<String> minMaxDataFile = getSourceFiles(minMaxDataSourceName);
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);
        GenericRecord minMaxDataGenericRecord = AvroUtils.getData(config, minMaxDataFile).get(0);
        Object minMaxObj = minMaxDataGenericRecord.get(AccountMasterStatsParameters.MIN_MAX_KEY);
        String minMaxData = minMaxObj instanceof Utf8 //
                ? ((Utf8) minMaxObj).toString() //
                : (String) minMaxObj;
        return minMaxData;
    }

    private Map<String, List<Object>> parseMinMaxInfo(String minMaxObjStr) {
        Map<String, List<Object>> minMaxInfo = new HashMap<>();

        if (minMaxObjStr != null) {
            Map<?, ?> tempMinMaxInfoMap1 = JsonUtils.deserialize(minMaxObjStr, Map.class);
            @SuppressWarnings("rawtypes")
            Map<String, List> tempMinMaxInfoMap2 = //
                    JsonUtils.convertMap(tempMinMaxInfoMap1, String.class, List.class);
            minMaxInfo = new HashMap<>();
            for (String key : tempMinMaxInfoMap2.keySet()) {
                List<Object> minMaxList = //
                        JsonUtils.convertList(tempMinMaxInfoMap2.get(key), Object.class);
                minMaxInfo.put(key, minMaxList);
            }
        }
        return minMaxInfo;
    }

    private Node createLeafFieldSubstitutionNode(Node node, //
            AccountMasterStatsParameters parameters, //
            String[] dimensionIdFieldNames, //
            boolean isNumericalBucketsRequired, //
            Map<String, List<Object>> minMaxInfo) {

        List<FieldMetadata> dimensionFieldMetadataList = new ArrayList<>();
        for (FieldMetadata fieldMeta : node.getSchema()) {
            boolean isDimension = false;
            String fieldName = fieldMeta.getFieldName();

            for (String dimensionFieldName : dimensionIdFieldNames) {
                if (fieldName.equals(dimensionFieldName)) {
                    isDimension = true;
                    break;
                }
            }
            if (isDimension) {
                dimensionFieldMetadataList.add(fieldMeta);
            }
        }

        AMStatsLeafFieldSubstitutionFunction.Params functionParams = //
                new AMStatsLeafFieldSubstitutionFunction.Params(//
                        getFields(node.getSchema()), //
                        parameters, //
                        dimensionFieldMetadataList, //
                        node.getFieldNames(), //
                        minMaxInfo, //
                        new HashSet<>(parameters.getHqDunsRelatedColumns()));

        AMStatsLeafFieldSubstitutionFunction leafCreationFunction = //
                new AMStatsLeafFieldSubstitutionFunction(functionParams);

        node = node.apply(leafCreationFunction, //
                getFieldList(node.getSchema()), //
                getTargetSchema(node.getSchema(), String.class), //
                getFieldList(node.getSchema()), //
                Fields.REPLACE);

        return node;
    }

    private List<FieldMetadata> getTargetSchema(List<FieldMetadata> fieldMetadataList, Class<?> javaType) {
        List<FieldMetadata> fields = new ArrayList<>();
        for (FieldMetadata field : fieldMetadataList) {
            FieldMetadata targetField = new FieldMetadata(field.getFieldName(), javaType);
            fields.add(targetField);
        }
        return fields;
    }
}
