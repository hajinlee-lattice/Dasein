package com.latticeengines.dataflow.exposed.builder.operations;

import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.KEY_ATTR;
import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.SUPPORTED_CLASSES;
import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.constructDeclaration;
import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.reservedAttrType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.KVAttrPicker;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.DepivotFunction;
import com.latticeengines.dataflow.runtime.cascading.KVReconstuctAggregator;
import com.latticeengines.dataflow.runtime.cascading.leadprioritization.KVAttrPickAggregator;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class KVOperation extends Operation {

    private static Set<String> SUPPORTED_CLASS_NAMES;

    static {
        SUPPORTED_CLASS_NAMES = new HashSet<>();
        SUPPORTED_CLASSES.forEach(clz -> SUPPORTED_CLASS_NAMES.add(clz.getSimpleName()));
    }

    // this is to depivot
    public KVOperation(Input input, FieldList fieldsToAppend, FieldList fieldsNotToPivot) {
        Pipe prior = input.pipe;
        Map<String, FieldMetadata> priorMap = DataFlowUtils.getFieldMetadataMap(input.metadata);

        Map<String, String> argTypeMap = getArgTypeMap(priorMap, fieldsNotToPivot);
        DepivotStrategy depivotStrategy = new KVDepivotStrategy(argTypeMap, fieldsToAppend, fieldsNotToPivot);

        List<FieldMetadata> fms = constructSchema(argTypeMap, fieldsToAppend, fieldsNotToPivot, priorMap);
        Fields fields = DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(fms));
        DepivotFunction function = new DepivotFunction(depivotStrategy, fields);

        this.pipe = new Each(prior, function, Fields.RESULTS);
        this.metadata = fms;
    }

    // this is to pick
    public KVOperation(Input input, String rowIdField, KVAttrPicker picker) {
        Pipe prior = input.pipe;
        Fields fields = DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(input.metadata));
        Aggregator agg = new KVAttrPickAggregator(fields, picker);
        Pipe groupBy  = new GroupBy(prior, new Fields(rowIdField, KEY_ATTR));
        this.pipe = new Every(groupBy, agg, Fields.RESULTS);
        this.metadata = input.metadata;
    }

    // this is to pivot
    public KVOperation(Input input, String rowIdField, List<FieldMetadata> outputFieldsExceptRowId) {
        Pipe prior = input.pipe;

        List<FieldMetadata> outputFields = new ArrayList<>();
        for (FieldMetadata fm: input.metadata) {
            if (rowIdField.equals(fm.getFieldName())) {
                outputFields.add(fm);
            }
        }
        outputFields.addAll(outputFieldsExceptRowId);

        Map<String, String> fieldTypes = new HashMap<>();
        for (FieldMetadata fm: outputFieldsExceptRowId) {
            fieldTypes.put(fm.getFieldName(), fm.getJavaType().getSimpleName());
        }

        Fields fields = DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(outputFields));
        Aggregator agg = new KVReconstuctAggregator(fields, fieldTypes);
        Pipe groupBy = new GroupBy(prior, new Fields(rowIdField));
        this.pipe = new Every(groupBy, agg, Fields.RESULTS);
        this.metadata = outputFields;
    }

    private static Map<String, String> getArgTypeMap(Map<String, FieldMetadata> priorMap, FieldList fieldsNotToPivot) {
        Set<String> skipFieldsSet = Collections.emptySet();
        if (fieldsNotToPivot != null) {
            skipFieldsSet = new HashSet<>(fieldsNotToPivot.getFieldsAsList());
        }
        Map<String, String> argTypeMap = new HashMap<>();
        for (Map.Entry<String, FieldMetadata> entry : priorMap.entrySet()) {
            String key = entry.getKey();
            Class<?> clz = entry.getValue().getJavaType();
            if (skipFieldsSet.contains(key)) {
                continue;
            }
            if (clzIsSupported(clz)) {
                argTypeMap.put(entry.getKey(), clz.getSimpleName());
            } else {
                throw new IllegalArgumentException("Field " + entry.getKey() + " of type "
                        + entry.getValue().getJavaType().getSimpleName() + " is not supported, do you want to skip it for pivoting?");
            }
        }
        return argTypeMap;
    }

    private static List<FieldMetadata> constructSchema(Map<String, String> argTypeMap, FieldList fieldsToAppend,
            FieldList fieldsToSkip, Map<String, FieldMetadata> priorMap) {
        Fields fields = constructDeclaration(argTypeMap, fieldsToAppend);
        Set<String> skipFieldsSet = new HashSet<>(fieldsToSkip.getFieldsAsList());

        List<FieldMetadata> fms = new ArrayList<>();
        for (Comparable comp : fields) {
            String fieldName = (String) comp;
            if (priorMap.containsKey(fieldName)) {
                fms.add(priorMap.get(fieldName));
            } else {
                // must be reserved field
                fms.add(new FieldMetadata(fieldName, reservedAttrType(fieldName)));
            }
        }

        return fms;
    }

    private static boolean clzIsSupported(Class<?> clz) {
        return clzIsSupported(clz.getSimpleName());
    }

    private static boolean clzIsSupported(String clzName) {
        return SUPPORTED_CLASS_NAMES.contains(clzName);
    }

}
