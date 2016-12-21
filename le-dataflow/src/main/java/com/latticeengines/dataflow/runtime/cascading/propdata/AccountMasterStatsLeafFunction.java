package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterStatsLeafFunction extends BaseOperation implements Function {
    private List<String> leafSchemaNewColumnNames;
    private Map<String, List<String>> dimensionDefinitionMap;
    private Map<String, String> reverseDimensionDefinitionMap;
    private Map<String, Long> requiredDimensionsRootAttrId;
    private Map<String, Map<String, Long>> requiredDimensionsValues;
    private String dimensionColumnPrepostfix;

    public AccountMasterStatsLeafFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        leafSchemaNewColumnNames = new ArrayList<String>();
        requiredDimensionsRootAttrId = new HashMap<>();
        reverseDimensionDefinitionMap = new HashMap<>();
        for (String dim : parameterObject.requiredDimensions.keySet()) {
            requiredDimensionsRootAttrId.put(dim, parameterObject.requiredDimensions.get(dim).getRootAttrId());
        }
        this.requiredDimensionsValues = new HashMap<>();
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;

        for (String dim : parameterObject.requiredDimensionsValuesMap.keySet()) {
            Map<String, Long> valuesIdMap = new HashMap<>();
            for (String valKey : parameterObject.requiredDimensionsValuesMap.get(dim).keySet()) {
                valuesIdMap.put(valKey, parameterObject.requiredDimensionsValuesMap.get(dim).get(valKey).getPid());
            }
            requiredDimensionsValues.put(dim, valuesIdMap);
        }

        for (FieldMetadata metadata : parameterObject.leafSchemaNewColumns) {
            leafSchemaNewColumnNames.add(metadata.getFieldName());
        }
        this.dimensionDefinitionMap = parameterObject.dimensionDefinitionMap;

        for (String key : parameterObject.dimensionDefinitionMap.keySet()) {
            List<String> subDimensions = parameterObject.dimensionDefinitionMap.get(key);
            for (String subDimension : subDimensions) {
                reverseDimensionDefinitionMap.put(subDimension, key);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Tuple result = Tuple.size(leafSchemaNewColumnNames.size());
        Map<String, Map<String, Long>> dimensionFieldValuesMap = new HashMap<>();
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            dimensionFieldValuesMap.put(dimensionKey, new HashMap<String, Long>());
        }

        TupleEntry entry = functionCall.getArguments();

        Tuple tuple = entry.getTuple();
        Fields fields = entry.getFields();

        Iterator<Object> itr = fields.iterator();
        int pos = 0;
        while (itr.hasNext()) {

            String field = (String) itr.next();

            if (reverseDimensionDefinitionMap.containsKey(field)) {
                Object value = tuple.getObject(pos);
                String dimensionKey = reverseDimensionDefinitionMap.get(field);
                if (dimensionKey.startsWith(dimensionColumnPrepostfix)
                        && dimensionKey.endsWith(dimensionColumnPrepostfix)) {
                    dimensionKey = dimensionKey.substring(dimensionColumnPrepostfix.length(),
                            dimensionKey.length() - dimensionColumnPrepostfix.length());
                }

                Map<String, Long> dimensionValues = dimensionFieldValuesMap
                        .get(reverseDimensionDefinitionMap.get(field));

                dimensionValues.put(field,
                        value == null || requiredDimensionsValues.get(dimensionKey) == null
                                || !requiredDimensionsValues.get(dimensionKey).containsKey(value)
                                        ? requiredDimensionsRootAttrId.get(dimensionKey) //
                                        : requiredDimensionsValues.get(dimensionKey).get(value));
                pos++;
            }
        }

        updateResult(result, dimensionFieldValuesMap, functionCall.getDeclaredFields());
        functionCall.getOutputCollector().add(result);
    }

    private void updateResult(Tuple result, Map<String, Map<String, Long>> dimensionFieldValuesMap, Fields fields) {
        for (String dimension : leafSchemaNewColumnNames) {
            Map<String, Long> dimensionValues = dimensionFieldValuesMap.get(dimension);
            Long dimensionId = dimensionValues.values().iterator().next();
            int pos = fields.getPos(dimension);
            result.set(pos, dimensionId);
        }
    }

    public static class Params {
        public Fields fieldDeclaration;
        public List<FieldMetadata> leafSchemaNewColumns;
        public List<FieldMetadata> leafSchemaOldColumns;
        public Map<String, List<String>> dimensionDefinitionMap;
        public Map<String, CategoricalDimension> requiredDimensions;
        public Map<String, ColumnMetadata> columnMetadatasMap;
        public Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;
        public String dimensionColumnPrepostfix;

        public Params(Fields fieldDeclaration, List<FieldMetadata> leafSchemaNewColumns,
                List<FieldMetadata> leafSchemaOldColumns, Map<String, List<String>> dimensionDefinitionMap,
                Map<String, CategoricalDimension> requiredDimensions, Map<String, ColumnMetadata> columnMetadatasMap,
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap,
                String dimensionColumnPrepostfix) {
            this.fieldDeclaration = fieldDeclaration;
            this.leafSchemaNewColumns = leafSchemaNewColumns;
            this.leafSchemaOldColumns = leafSchemaOldColumns;
            this.dimensionDefinitionMap = dimensionDefinitionMap;
            this.requiredDimensions = requiredDimensions;
            this.columnMetadatasMap = columnMetadatasMap;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
        }
    }
}
