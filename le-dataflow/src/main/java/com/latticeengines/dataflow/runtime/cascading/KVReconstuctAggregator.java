package com.latticeengines.dataflow.runtime.cascading;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class KVReconstuctAggregator extends BaseAggregator<KVReconstuctAggregator.Context> //
        implements Aggregator<KVReconstuctAggregator.Context> {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> fieldTypes;

    public static class Context extends BaseAggregator.Context
    {
        ConcurrentMap<String, Object> row = new ConcurrentHashMap<>();
        ConcurrentMap<String, String> fieldsToFind = new ConcurrentHashMap<>();
    }

    public KVReconstuctAggregator(Fields fieldDeclaration, Map<String, String> fieldTypes) {
        super(fieldDeclaration);
        this.fieldTypes = fieldTypes;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        // any group by value is either null or empty string
        for (Object grpObj: group.asIterableOf(Object.class)) {
            if (grpObj == null) {
                return true;
            }
            if (grpObj instanceof Utf8 && StringUtils.isBlank(grpObj.toString())) {
                return true;
            }
            if (grpObj instanceof String && StringUtils.isBlank((String) grpObj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.fieldsToFind.putAll(this.fieldTypes);
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        for (Map.Entry<String, String> entry: context.fieldsToFind.entrySet()) {
            String tgtFieldName = entry.getKey();
            String tgtFieldValue = (String) arguments.getObject(KVDepivotStrategy.KEY_ATTR);
            
            if (!tgtFieldName.equals(tgtFieldValue)) {
                continue;
            }
            String srcFieldName = KVDepivotStrategy.valueAttr(entry.getValue());
            Object srcValue = arguments.getObject(srcFieldName);
            
            if (srcValue != null) {
                context.row.put(tgtFieldName, srcValue);
                context.fieldsToFind.remove(tgtFieldName);
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple tuple = Tuple.size(fieldDeclaration.size());
        TupleEntry group = context.groupTuple;
        for (int i = 0; i < group.size(); i++) {
            String fieldName = (String) group.getFields().get(i);
            Integer idx = namePositionMap.get(fieldName);
            tuple.set(idx, group.getObject(i));
        }
        for (Map.Entry<String, Object> entry: context.row.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            Integer idx = namePositionMap.get(fieldName);
            tuple.set(idx, value);
        }
        return tuple;
    }

}
