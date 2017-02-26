package com.latticeengines.dataflow.runtime.cascading.leadprioritization;

import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.valueAttr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.exposed.builder.strategy.KVAttrPicker;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class KVAttrPickAggregator
        extends BaseAggregator<KVAttrPickAggregator.Context>
        implements Aggregator<KVAttrPickAggregator.Context> {

    public static class Context extends BaseAggregator.Context {
        public Object value = null;
        public ConcurrentMap<String, Object> helpFields = new ConcurrentHashMap<>();
        public boolean foundAlready = false;
    }

    private final String valAttr;
    private final Collection<String> helpFields;
    private final KVAttrPicker picker;

    public KVAttrPickAggregator(Fields fieldDeclaration, KVAttrPicker picker) {
        super(fieldDeclaration);
        this.valAttr = valueAttr(picker.valClzSimpleName());
        this.helpFields = picker.helpFieldNames();
        this.picker = picker;
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

    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    protected Context updateContext(Context context, TupleEntry arguments) {
        if (!context.foundAlready) {
            Map<String, Object> newHelp = new HashMap<>();
            for (String fieldName : helpFields) {
                newHelp.put(fieldName, arguments.getObject(fieldName));
            }
            context.value = picker.updateHelpAndReturnValue(context.value, context.helpFields, arguments.getObject(valAttr), newHelp);
        }
        return context;
    }

    protected Tuple finalizeContext(Context context) {
        Tuple tuple = Tuple.size(fieldDeclaration.size());

        TupleEntry group = context.groupTuple;
        for (int i = 0; i < group.size(); i++) {
            String fieldName = (String) group.getFields().get(i);
            Integer idx = namePositionMap.get(fieldName);
            tuple.set(idx, group.getObject(i));
        }

        for (Map.Entry<String, Object> entry: context.helpFields.entrySet()) {
            String fieldName = entry.getKey();
            Integer idx = namePositionMap.get(fieldName);
            tuple.set(idx, entry.getValue());
        }

        tuple.set(namePositionMap.get(valAttr), context.value);

        return tuple;
    }

}
