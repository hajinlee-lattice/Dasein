package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMStatsLeafFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = -4039806083023012431L;

    private Map<String, List<String>> dimensionDefinitionMap;
    private Map<String, String> reverseDimensionDefinitionMap;
    private Map<String, Long> requiredDimensionsRootAttrId;
    private Map<String, Map<String, Long>> requiredDimensionsValues;
    private String dimensionColumnPrepostfix;

    public AMStatsLeafFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        requiredDimensionsRootAttrId = new HashMap<>();
        reverseDimensionDefinitionMap = new HashMap<>();
        for (String dim : parameterObject.requiredDimensions.keySet()) {
            requiredDimensionsRootAttrId.put(dim, //
                    parameterObject.requiredDimensions.get(dim).getRootAttrId());
        }
        this.requiredDimensionsValues = new HashMap<>();
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;

        for (String dim : parameterObject.requiredDimensionsValuesMap.keySet()) {
            Map<String, Long> valuesIdMap = new HashMap<>();
            Map<String, CategoricalAttribute> dimensionValueIdMap = //
                    parameterObject.requiredDimensionsValuesMap.get(dim);

            for (String valKey : dimensionValueIdMap.keySet()) {
                valuesIdMap.put(valKey, //
                        dimensionValueIdMap.get(valKey).getPid());
            }

            requiredDimensionsValues.put(dim, valuesIdMap);
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
        Tuple result = Tuple.size(requiredDimensionsValues.size());
        Map<String, Map<String, Long>> dimensionFieldValuesMap = new HashMap<>();
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            dimensionFieldValuesMap.put(dimensionKey, new HashMap<String, Long>());
        }

        TupleEntry entry = functionCall.getArguments();

        Fields fields = entry.getFields();

        Iterator<Object> itr = fields.iterator();
        while (itr.hasNext()) {
            String field = (String) itr.next();
            if (reverseDimensionDefinitionMap.containsKey(field)) {
                Object value = entry.getObject(field);
                String dimensionKey = reverseDimensionDefinitionMap.get(field);
                if (dimensionKey.startsWith(dimensionColumnPrepostfix)
                        && dimensionKey.endsWith(dimensionColumnPrepostfix)) {
                    dimensionKey = dimensionKey.substring(dimensionColumnPrepostfix.length(),
                            dimensionKey.length() - dimensionColumnPrepostfix.length());
                }

                Map<String, Long> dimensionValues = dimensionFieldValuesMap
                        .get(reverseDimensionDefinitionMap.get(field));

                Long dimensionValue = (value == null || //
                        requiredDimensionsValues.get(dimensionKey) == null//
                        || !requiredDimensionsValues.get(dimensionKey).containsKey(value))//
                                ? //
                                requiredDimensionsRootAttrId.get(dimensionKey) * 100000//
                                : requiredDimensionsValues.get(dimensionKey).get(value);

                dimensionValues.put(field, dimensionValue);
            }
        }

        updateResult(result, dimensionFieldValuesMap, functionCall.getDeclaredFields());
        functionCall.getOutputCollector().add(result);
    }

    private void updateResult(Tuple result, Map<String, Map<String, Long>> dimensionFieldValuesMap, Fields fields) {
        for (String dimension : dimensionDefinitionMap.keySet()) {
            Map<String, Long> dimensionValues = dimensionFieldValuesMap.get(dimension);
            Long dimensionId = dimensionValues.values().iterator().next();
            int pos = fields.getPos(dimension);
            result.set(pos, dimensionId);
        }
    }

    public static class Params {
        Fields fieldDeclaration;
        Map<String, List<String>> dimensionDefinitionMap;
        Map<String, CategoricalDimension> requiredDimensions;
        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;
        String dimensionColumnPrepostfix;

        public Params(Fields fieldDeclaration, Map<String, List<String>> dimensionDefinitionMap, //
                Map<String, CategoricalDimension> requiredDimensions,
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap) {
            this.fieldDeclaration = fieldDeclaration;
            this.dimensionDefinitionMap = dimensionDefinitionMap;
            this.requiredDimensions = requiredDimensions;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
            this.dimensionColumnPrepostfix = AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX;
        }
    }
}
