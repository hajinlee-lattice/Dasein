package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionUtil.ExpandedTuple;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.Dimensions;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.MultiListCrossProductUtil;
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
public class AMStatsDedupAggRollupWithHQDuns extends BaseOperation implements Buffer {

    private static final Log log = LogFactory.getLog(AMStatsDedupAggRollupWithHQDuns.class);

    private static final long serialVersionUID = 4217950767704131475L;
    private static final int MAX_DEPTH = 5;
    private AMStatsDimensionUtil dimensionUtil;
    private MultiListCrossProductUtil crossProductUtil;

    private Map<Long, List<Long>> dimensionValueAncestorPathMap;

    private List<String> dimensionFields;
    private List<Integer> dimFieldPosList;
    private List<Integer> hqDunsFieldsPosList;

    public AMStatsDedupAggRollupWithHQDuns(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.dimensionFields = parameterObject.dimensionFields;
        dimensionUtil = new AMStatsDimensionUtil();
        crossProductUtil = new MultiListCrossProductUtil();
        dimFieldPosList = new ArrayList<>();

        dimensionValueAncestorPathMap = new HashMap<>();

        for (String dimensionField : dimensionFields) {
            int idx = 0;
            for (String field : parameterObject.fields) {
                if (field.toString().equals(dimensionField)) {
                    dimFieldPosList.add(idx);
                    break;
                }
                idx++;
            }
        }

        for (Map<String, CategoricalAttribute> expandDimensionFieldValuesMap : parameterObject.requiredDimensionsValuesMap
                .values()) {
            calculateDimensionValueAncestorPathMap(expandDimensionFieldValuesMap);
        }

        hqDunsFieldsPosList = new ArrayList<>();

        for (String fieldHQ : parameterObject.hqDunsFields) {
            int idx = 0;
            for (String field : parameterObject.fields) {
                if (field.toString().equals(fieldHQ)) {
                    hqDunsFieldsPosList.add(idx);
                    break;
                }
                idx++;
            }
        }

    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        @SuppressWarnings("unchecked")

        // extract values of hq duns and domain fields

        // extract values of all dimensions

        // run dedup for matching dimensions

        // after this follow dimensionValue ancestor path map to generate rollup
        // entries and run dedup

        // finally write all deduped records

        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        List<Object> hqDunsFieldValues = null;
        Fields fields = bufferCall.getArgumentFields();
        int fieldLength = fields.getComparators().length;

        Map<Dimensions, ExpandedTuple> expandedTupleMap = new HashMap<>();

        TupleEntryCollector outputCollector = bufferCall.getOutputCollector();
        String hqDunsFieldValuesStr = "";

        while (argumentsInGroup.hasNext()) {

            TupleEntry arguments = argumentsInGroup.next();
            Tuple originalTuple = arguments.getTuple();

            List<Long> dimValues = new ArrayList<>();
            for (Integer dimPos : dimFieldPosList) {
                dimValues.add(originalTuple.getLong(dimPos));
            }

            if (hqDunsFieldValues == null) {
                hqDunsFieldValues = new ArrayList<>();
                for (Integer hqPos : hqDunsFieldsPosList) {
                    hqDunsFieldValues.add(originalTuple.getObject(hqPos));
                    hqDunsFieldValuesStr += " [" + originalTuple.getObject(hqPos) + "] ";
                }

            }

            ExpandedTuple expandedTuple = new ExpandedTuple(originalTuple, fieldLength);
            dedupAndPut(fieldLength, expandedTupleMap, new Dimensions(dimValues), expandedTuple);
        }

        log.debug("hqDunsFieldValuesStr: " + hqDunsFieldValuesStr);

        Set<Dimensions> dimSet = new HashSet<>();
        // to avoid ConcurrentModificationException copy key set in separate set
        // to use in for loop which tries to update this map
        for (Dimensions dim : expandedTupleMap.keySet()) {
            if (expandedTupleMap.keySet().size() > 5000) {
                log.debug("Printing dimensions as map contains lot of elements (" + expandedTupleMap.keySet().size()
                        + "): " + dim.toString() + ", hqDunsFieldValuesStr: " + hqDunsFieldValuesStr);
            }
            dimSet.add(dim);
        }

        for (Dimensions dim : dimSet) {
            ExpandedTuple expandedTuple = expandedTupleMap.get(dim);
            Map<Long, List<Long>> dimAncestorMap = new HashMap<>();
            for (Long dimId : dim.getDimensions()) {
                List<Long> ancestorsPids = dimensionValueAncestorPathMap.get(dimId);
                dimAncestorMap.put(dimId, ancestorsPids);
            }

            List<List<Long>> dimensionCombinations = calculationDimensionCombinations(dimAncestorMap,
                    dim.getDimensions());

            for (List<Long> ancestorTuple : dimensionCombinations) {
                ExpandedTuple rollupExpandedTuple = new ExpandedTuple(expandedTuple);
                int idx = 0;
                for (Integer dimPos : dimFieldPosList) {
                    rollupExpandedTuple.set(dimPos, ancestorTuple.get(idx++));
                }
                dedupAndPut(fieldLength, expandedTupleMap, new Dimensions(ancestorTuple), rollupExpandedTuple);
            }
        }

        for (Dimensions dim : expandedTupleMap.keySet()) {
            outputCollector.add(expandedTupleMap.get(dim).generateTuple());
        }
    }

    private void dedupAndPut(int fieldLength, Map<Dimensions, ExpandedTuple> expandedTupleMap, //
            Dimensions dim, ExpandedTuple expandedTuple) {
        ExpandedTuple dedupedTuple = expandedTuple;

        if (expandedTupleMap.containsKey(dim)) {
            log.debug("Dimension key already exists in map, calling dedup: " + dim.toString());
            ExpandedTuple existingExpandedTuple = expandedTupleMap.get(dim);
            dedupedTuple = dimensionUtil.dedup(existingExpandedTuple, expandedTuple, fieldLength);
        }
        expandedTupleMap.put(dim, dedupedTuple);
    }

    private List<List<Long>> calculationDimensionCombinations(Map<Long, List<Long>> dimAncestorMap,
            List<Long> dimensions) {
        List<List<Long>> inputLists = new ArrayList<>();
        for (Long dim : dimensions) {
            List<Long> ancestorList = dimAncestorMap.get(dim);
            List<Long> dimensionList = new ArrayList<>();
            if (ancestorList.size() == 0 || !ancestorList.get(0).equals(dim)) {
                dimensionList.add(dim);
            }
            dimensionList.addAll(ancestorList);
            inputLists.add(dimensionList);
        }

        List<List<Long>> result = crossProductUtil.calculateCross(inputLists);
        // this util makes sure that first entry in the result contains first
        // elements from all the lists, this allows us to easily disregard
        // lowest most dimension combination in stats rollup logic
        result.remove(0);

        return result;
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
        Set<Long> ancestorPathSet = new HashSet<Long>(ancestorPath);

        int depth = 0;
        while (depth < MAX_DEPTH) {
            // traverse tree till reach root
            Long topMostKnownAncestor = ancestorPath.get(ancestorPath.size() - 1);
            if (rootAncestorId.equals(topMostKnownAncestor)) {
                break;
            } else {
                for (Long parent : dimensionValueAncestorPathMap.get(topMostKnownAncestor)) {
                    if (!ancestorPathSet.contains(parent)) {
                        ancestorPathSet.add(parent);
                        ancestorPath.add(parent);
                    }
                }
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
        List<String> fields;
        List<String> dimensionFields;
        List<String> hqDunsFields;
        Fields fieldDeclaration;
        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;

        public Params(List<String> fields, //
                List<String> dimensionFields, //
                List<String> hqDunsFields, //
                Fields fieldDeclaration, //
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap) {
            this.fields = fields;
            this.dimensionFields = dimensionFields;
            this.hqDunsFields = hqDunsFields;
            this.fieldDeclaration = fieldDeclaration;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
        }
    }
}
