package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionUtil.ExpandedTuple;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("rawtypes")
public class AMStatsDimensionExpandBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;
    private static final int MAX_DEPTH = 5;
    private AMStatsDimensionUtil dimensionUtil;

    private Map<Long, List<Long>> dimensionValueAncestorPathMap;

    private String expandField;

    public AMStatsDimensionExpandBuffer(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.expandField = parameterObject.expandField;
        dimensionUtil = new AMStatsDimensionUtil();

        dimensionValueAncestorPathMap = new HashMap<>();

        String normalizedExpandField = parameterObject.expandField//
                .substring(parameterObject.dimensionColumnPrepostfix.length(), //
                        parameterObject.expandField.length() //
                                - parameterObject.dimensionColumnPrepostfix.length());

        Map<String, CategoricalAttribute> expandDimensionFieldValuesMap = //
                parameterObject.requiredDimensionsValuesMap.get(normalizedExpandField);

        calculateDimensionValueAncestorPathMap(expandDimensionFieldValuesMap);

    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        @SuppressWarnings("unchecked")
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        Map<Long, ExpandedTuple> tuplesMap = new HashMap<>();
        Comparator[] fieldsArray = bufferCall.getArgumentFields().getComparators();
        int fieldsLength = fieldsArray.length;

        Integer pos = null;
        TupleEntryCollector outputCollector = bufferCall.getOutputCollector();

        while (argumentsInGroup.hasNext()) {

            TupleEntry arguments = argumentsInGroup.next();
            Tuple originalTuple = arguments.getTuple();

            if (pos == null) {
                Iterator itr = arguments.getFields().iterator();
                int idx = 0;
                while (itr.hasNext()) {
                    String name = (String) itr.next();
                    if (name.equals(expandField)) {
                        pos = idx;
                        break;
                    }
                    idx++;
                }
            }

            Long nonFixedDimensionId = originalTuple.getLong(pos);

            List<Long> ancestorList = dimensionValueAncestorPathMap.get(nonFixedDimensionId);
            mergeAndPutTuple(fieldsArray, tuplesMap, //
                    new ExpandedTuple(originalTuple, fieldsLength), //
                    nonFixedDimensionId);

            if (ancestorList != null //
                    && ancestorList.size() != 0//
                    && !nonFixedDimensionId.equals(ancestorList.get(0))) {
                for (Long ancestorId : ancestorList) {
                    if (!nonFixedDimensionId.equals(ancestorId)) {
                        ExpandedTuple rollupTuple = new ExpandedTuple(originalTuple, fieldsLength);
                        rollupTuple.set(pos, ancestorId);
                        mergeAndPutTuple(fieldsArray, tuplesMap, //
                                rollupTuple, ancestorId);
                    }
                }
            }
        }

        for (Long id : tuplesMap.keySet()) {
            ExpandedTuple tuple = tuplesMap.get(id);
            outputCollector.add(tuple.generateTuple());
        }
    }

    private void mergeAndPutTuple(Comparator[] fieldsArray, //
            Map<Long, ExpandedTuple> tuplesMap, ExpandedTuple originalTuple, //
            Long nonFixedDimension) {

        if (tuplesMap.containsKey(nonFixedDimension)) {
            ExpandedTuple existingMergedTuple = tuplesMap.get(nonFixedDimension);
            tuplesMap.put(nonFixedDimension,
                    dimensionUtil.merge(existingMergedTuple, originalTuple, fieldsArray.length));
        } else {
            tuplesMap.put(nonFixedDimension, originalTuple);
        }
    }

    private void calculateDimensionValueAncestorPathMap(
            Map<String, CategoricalAttribute> expandDimensionFieldValuesMap) {
        Long rootAncestorId = null;

        for (String valKey : expandDimensionFieldValuesMap.keySet()) {
            rootAncestorId = calculateFirstLevelAncestorPath(//
                    expandDimensionFieldValuesMap, rootAncestorId, valKey);
        }

        for (Long pid : dimensionValueAncestorPathMap.keySet()) {
            calculateAncestorPathTillRoot(rootAncestorId, pid);
        }
    }

    private void calculateAncestorPathTillRoot(Long rootAncestorId, Long pid) {
        List<Long> ancestorPath = dimensionValueAncestorPathMap.get(pid);
        int depth = 0;
        while (depth < MAX_DEPTH) {
            // traverse tree till reach root
            Long topMostKnownAncestor = ancestorPath.get(ancestorPath.size() - 1);
            if (rootAncestorId.equals(topMostKnownAncestor)) {
                break;
            } else {
                ancestorPath.addAll(dimensionValueAncestorPathMap.get(topMostKnownAncestor));
            }

            depth++;
        }
    }

    private Long calculateFirstLevelAncestorPath(//
            Map<String, CategoricalAttribute> expandDimensionFieldValuesMap, //
            Long rootAncestorId, String valKey) {
        CategoricalAttribute attr = expandDimensionFieldValuesMap.get(valKey);

        List<Long> ancestorPath = new ArrayList<>();

        Long parentId = attr.getParentId();
        if (parentId == null) {
            parentId = attr.getPid();
        }

        if (rootAncestorId == null && attr.getPid().equals(parentId)) {
            rootAncestorId = parentId;
        }

        ancestorPath.add(parentId);

        dimensionValueAncestorPathMap.put(attr.getPid(), ancestorPath);
        return rootAncestorId;
    }

    public static class Params {
        public String expandField;
        public Map<String, List<String>> hierarchicalDimensionTraversalMap;
        public Fields fieldDeclaration;
        public Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;
        public String dimensionColumnPrepostfix;

        public Params(String expandField, //
                Map<String, List<String>> hierarchicalDimensionTraversalMap, //
                Fields fieldDeclaration, //
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap) {
            this.expandField = expandField;
            this.hierarchicalDimensionTraversalMap = hierarchicalDimensionTraversalMap;
            this.fieldDeclaration = fieldDeclaration;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
            this.dimensionColumnPrepostfix = AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX;
        }
    }
}
