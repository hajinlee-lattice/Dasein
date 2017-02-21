package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttributeStatsDetailsMergeUtil;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterStatsDimensionExpandBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;
    private static final int MAX_DEPTH = 5;
    private static ObjectMapper OM = new ObjectMapper();

    private Map<Long, List<Long>> dimensionValueAncestorPathMap;
    private String normalizedExpandField;
    private String expandField;

    public AccountMasterStatsDimensionExpandBuffer(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.expandField = parameterObject.expandField;

        dimensionValueAncestorPathMap = new HashMap<>();
        normalizedExpandField = parameterObject.expandField.substring(
                parameterObject.dimensionColumnPrepostfix.length(),
                parameterObject.expandField.length() - parameterObject.dimensionColumnPrepostfix.length());

        Map<String, CategoricalAttribute> expandDimensionFieldValuesMap = //
                parameterObject.requiredDimensionsValuesMap.get(normalizedExpandField);

        calculateDimensionValueAncestorPathMap(expandDimensionFieldValuesMap);

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

    private Long calculateFirstLevelAncestorPath(Map<String, CategoricalAttribute> expandDimensionFieldValuesMap,
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

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        @SuppressWarnings("unchecked")
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        Map<Long, Tuple> tuplesMap = new HashMap<>();
        Comparator[] fieldsArray = bufferCall.getArgumentFields().getComparators();

        Integer pos = null;
        while (argumentsInGroup.hasNext()) {

            TupleEntry arguments = argumentsInGroup.next();
            Tuple originalTuple = arguments.getTupleCopy();

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

            mergeAndPutTuple(fieldsArray, tuplesMap, originalTuple, nonFixedDimensionId);

            List<Long> ancestorList = dimensionValueAncestorPathMap.get(nonFixedDimensionId);
            if (ancestorList != null //
                    && ancestorList.size() != 0//
                    && !nonFixedDimensionId.equals(ancestorList.get(0))) {
                for (Long ancestorId : ancestorList) {
                    if (!nonFixedDimensionId.equals(ancestorId)) {
                        Tuple rollupTuple = arguments.getTupleCopy();

                        rollupTuple.setLong(pos, ancestorId);
                        mergeAndPutTuple(fieldsArray, tuplesMap, rollupTuple, ancestorId);
                    }
                }
            }
        }

        for (Long id : tuplesMap.keySet()) {
            Tuple tuple = tuplesMap.get(id);
            bufferCall.getOutputCollector().add(tuple);
        }
    }

    private void mergeAndPutTuple(Comparator[] fieldsArray, Map<Long, Tuple> tuplesMap, Tuple originalTuple,
            Long nonFixedDimension) {
        if (tuplesMap.containsKey(nonFixedDimension)) {
            Tuple existingMergedTuple = tuplesMap.get(nonFixedDimension);
            tuplesMap.put(nonFixedDimension, merge(fieldsArray, existingMergedTuple, originalTuple));
        } else {
            tuplesMap.put(nonFixedDimension, originalTuple);
        }
    }

    private Tuple merge(Comparator[] fieldsArray, Tuple existingMergedTuple, Tuple originalTuple) {
        AttributeStatsDetails mergedStats = null;
        for (int idx = 0; idx < fieldsArray.length; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.getObject(idx);
            Object objInOriginalTuple = originalTuple.getObject(idx);

            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof String) {
                AttributeStatsDetails statsInExistingMergedTuple = null;
                AttributeStatsDetails statsInOriginalTuple = null;

                try {
                    statsInExistingMergedTuple = OM.readValue((String) objInExistingMergedTuple,
                            AttributeStatsDetails.class);
                    statsInOriginalTuple = OM.readValue((String) objInOriginalTuple, AttributeStatsDetails.class);
                } catch (IOException e) {
                    // ignore if type of serialized obj is not
                    // statsInExistingMergedTuple
                    continue;
                }

                mergedStats = merge(statsInExistingMergedTuple, statsInOriginalTuple);
                try {
                    existingMergedTuple.set(idx, OM.writeValueAsString(mergedStats));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return existingMergedTuple;
    }

    private AttributeStatsDetails merge(AttributeStatsDetails statsInExistingMergedTuple,
            AttributeStatsDetails statsInOriginalTuple) {
        AttributeStatsDetails mergedAttrStats = AttributeStatsDetailsMergeUtil
                .addStatsDetails(statsInExistingMergedTuple, statsInOriginalTuple);
        return mergedAttrStats;
    }

    public static class Params {
        public String expandField;
        public Map<String, List<String>> hierarchicalDimensionTraversalMap;
        public Fields fieldDeclaration;
        public int pos;
        public Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;
        public String dimensionColumnPrepostfix;

        public Params(String expandField, Map<String, List<String>> hierarchicalDimensionTraversalMap,
                Fields fieldDeclaration, int pos,
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap,
                String dimensionColumnPrepostfix) {
            this.expandField = expandField;
            this.hierarchicalDimensionTraversalMap = hierarchicalDimensionTraversalMap;
            this.fieldDeclaration = fieldDeclaration;
            this.pos = pos;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
        }
    }
}
