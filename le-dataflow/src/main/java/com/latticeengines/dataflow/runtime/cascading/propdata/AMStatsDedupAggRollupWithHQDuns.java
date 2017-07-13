package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final Logger log = LoggerFactory.getLogger(AMStatsDedupAggRollupWithHQDuns.class);

    private static final long serialVersionUID = 4217950767704131475L;
    private static final int MAX_DEPTH = 5;
    private AMStatsDimensionUtil dimensionUtil;
    private MultiListCrossProductUtil crossProductUtil;

    private Map<Long, List<Long>> dimensionValueAncestorPathMap;
    private List<String> dimensionFields;

    private List<Integer> dimFieldPosList;
    private Map<Integer, Integer> inOutPosMap = new HashMap<>();

    public AMStatsDedupAggRollupWithHQDuns(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        dimensionFields = parameterObject.dimensionFields;
        dimensionUtil = new AMStatsDimensionUtil();
        crossProductUtil = new MultiListCrossProductUtil();
        dimFieldPosList = new ArrayList<>();

        dimensionValueAncestorPathMap = new HashMap<>();

        Collection<Map<String, CategoricalAttribute>> dimFieldValues = //
                parameterObject.requiredDimensionsValuesMap.values();

        for (Map<String, CategoricalAttribute> expandDimensionFieldValuesMap : dimFieldValues) {
            calculateDimensionValueAncestorPathMap(expandDimensionFieldValuesMap);
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
        Fields fields = bufferCall.getArgumentFields();
        int argFieldsLength = fields.getComparators().length;

        Map<Dimensions, ExpandedTuple> expandedTupleMap = new HashMap<>();

        TupleEntryCollector outputCollector = bufferCall.getOutputCollector();
        String hqDunsFieldValuesStr = "";

        TupleEntry group = bufferCall.getGroup();
        for (Object obj: group.asIterableOf(Object.class)) {
            hqDunsFieldValuesStr += " [" + obj + "] ";
        }

        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Tuple originalTuple = arguments.getTuple();
            initArgPos(arguments);

            List<Long> dimValues = new ArrayList<>();
            for (Integer dimPos : dimFieldPosList) {
                dimValues.add(originalTuple.getLong(dimPos));
            }

            ExpandedTuple expandedTuple = new ExpandedTuple(originalTuple);
            dedupAndPut(argFieldsLength, expandedTupleMap, new Dimensions(dimValues), expandedTuple);
        }

        Set<Dimensions> dimSet = new HashSet<>();
        // to avoid ConcurrentModificationException copy key set in separate set
        // to use in for loop which tries to update this map
        for (Dimensions dim : expandedTupleMap.keySet()) {
            dimSet.add(dim);
        }

        int max = 0;
        for (Dimensions dim : dimSet) {
            ExpandedTuple expandedTuple = expandedTupleMap.get(dim);
            Map<Long, List<Long>> dimAncestorMap = new HashMap<>();
            for (Long dimId : dim.getDimensions()) {
                List<Long> ancestorsPids = dimensionValueAncestorPathMap.get(dimId);
                dimAncestorMap.put(dimId, ancestorsPids);
            }

            List<List<Long>> dimensionCombinations = calculationDimensionCombinations(dimAncestorMap,
                    dim.getDimensions());

            if (max < dimensionCombinations.size()) {
                max = dimensionCombinations.size();
            }

            for (List<Long> ancestorTuple : dimensionCombinations) {
                ExpandedTuple rollupExpandedTuple = new ExpandedTuple(expandedTuple);
                int idx = 0;
                for (Integer dimPos : dimFieldPosList) {
                    rollupExpandedTuple.set(dimPos, ancestorTuple.get(idx++));
                }
                dedupAndPut(argFieldsLength, expandedTupleMap, new Dimensions(ancestorTuple), rollupExpandedTuple);
            }
        }

        for (Dimensions dim : expandedTupleMap.keySet()) {
            try {
                log.info(String.format("Writing output tuple: hqDunsFieldValuesStr = %s, Dimensions = %s",
                        hqDunsFieldValuesStr, dim.toString()));
                Tuple resultInInputOrder = expandedTupleMap.get(dim).generateTuple();
                Tuple result = Tuple.size(fieldDeclaration.size());
                for (int i = 0; i < resultInInputOrder.size(); i++) {
                    if (inOutPosMap.containsKey(i)) {
                        Object val = resultInInputOrder.getObject(i);
                        result.set(inOutPosMap.get(i), val);
                    }
                }
                outputCollector.add(result);
            } catch (Exception ex) {
                String msg = String.format(
                        "%s\nhqDunsFieldValuesStr=%s, Dimensions=%s, expandedTupleMapKeySet=%s, "
                                + "tupleSize=%d, outputCollectorSize=%d\n", //
                        ex.getMessage(), hqDunsFieldValuesStr, dim.toString(), expandedTupleMap.keySet(),
                        expandedTupleMap.get(dim).size(), bufferCall.getDeclaredFields().size());
                ObjectMapper om = new ObjectMapper();
                try {
                    msg = String.format("%s\n\n%s\n", msg,
                            om.writeValueAsString(expandedTupleMap.get(dim).generateTuple()));
                } catch (JsonProcessingException e) {
                    // ignore
                }
                throw new RuntimeException(msg, ex);
            }
        }
    }

    private void initArgPos(TupleEntry argument) {
        if (inOutPosMap.isEmpty()) {
            Map<String, Integer> outPos = new HashMap<>();
            for (int i = 0; i < fieldDeclaration.size(); i++) {
                String fieldName = (String) fieldDeclaration.get(i);
                outPos.put(fieldName, i);
            }
            Fields argFields = argument.getFields();
            for (int i =0; i < argFields.size(); i++) {
                String fieldName = (String) argFields.get(i);
                if (outPos.containsKey(fieldName)) {
                    inOutPosMap.put(i, outPos.get(fieldName));
                }
            }
        }

        if (dimFieldPosList.isEmpty()) {
            for (String dimField: dimensionFields) {
                Integer pos = argument.getFields().getPos(dimField);
                dimFieldPosList.add(pos);
            }
        }

    }

    private void dedupAndPut(int fieldLength, Map<Dimensions, ExpandedTuple> expandedTupleMap, //
            Dimensions dim, ExpandedTuple expandedTuple) {
        ExpandedTuple dedupedTuple = expandedTuple;

        if (expandedTupleMap.containsKey(dim)) {
            log.info(String.format("Dimension key already exists in map, calling dedup: %s", dim.toString()));
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
        List<String> dimensionFields;
        Fields fieldDeclaration;
        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;

        public Params(List<String> dimensionFields, //
                Fields fieldDeclaration, //
                Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap) {
            this.dimensionFields = dimensionFields;
            this.fieldDeclaration = fieldDeclaration;
            this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
        }
    }
}
