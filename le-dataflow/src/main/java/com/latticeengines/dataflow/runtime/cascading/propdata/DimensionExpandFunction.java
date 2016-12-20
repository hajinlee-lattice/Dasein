package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DimensionExpandFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 6395662991286452847L;
    private int pos;
    private Map<String, List<Long>> dimensionValues;

    public DimensionExpandFunction(Params parameterObject) {
        super(parameterObject.numArgs, parameterObject.fieldDeclaration);
        this.pos = parameterObject.pos;

        dimensionValues = new HashMap<>();
        String normalizedExpandField = parameterObject.expandField.substring(
                parameterObject.dimensionColumnPrepostfix.length(),
                parameterObject.expandField.length() - parameterObject.dimensionColumnPrepostfix.length());

        for (String valKey : parameterObject.requiredDimensionsValuesMap.get(normalizedExpandField).keySet()) {
            CategoricalAttribute attr = parameterObject.requiredDimensionsValuesMap.get(normalizedExpandField)
                    .get(valKey);

            List<Long> ancestorPath = new ArrayList<>();

            Long parentId = attr.getParentId();
            if (parentId == null) {
                parentId = attr.getPid();
            }

            ancestorPath.add(parentId);

            dimensionValues.put(attr.getPid() + "", ancestorPath);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry group = functionCall.getArguments();
        Tuple originalTuple = group.getTuple();
        functionCall.getOutputCollector().add(originalTuple);

        String id = originalTuple.getString(pos);

        for (Long ancestorId : dimensionValues.get(id)) {
            if (!id.equals(ancestorId + "")) {
                Tuple rollupTuple = group.getTupleCopy();

                rollupTuple.setString(pos, ancestorId + "");
                functionCall.getOutputCollector().add(rollupTuple);
            }
        }
    }

    public static class Params {
        public int numArgs;
        public String expandField;
        public Map<String, List<String>> hierarchicalDimensionTraversalMap;
        public Fields fieldDeclaration;
        public int pos;
        public Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;
        public String dimensionColumnPrepostfix;

        public Params(int numArgs, String expandField, Map<String, List<String>> hierarchicalDimensionTraversalMap,
                Fields fieldDeclaration, int pos,
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap,
                String dimensionColumnPrepostfix) {
            this.numArgs = numArgs;
            this.expandField = expandField;
            this.hierarchicalDimensionTraversalMap = hierarchicalDimensionTraversalMap;
            this.fieldDeclaration = fieldDeclaration;
            this.pos = pos;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
        }
    }
}
