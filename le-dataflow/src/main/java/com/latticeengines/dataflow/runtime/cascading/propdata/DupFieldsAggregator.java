package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DupFieldsAggregator extends BaseAggregator<DupFieldsAggregator.Context>
        implements Aggregator<DupFieldsAggregator.Context> {
    private static final long serialVersionUID = -213929536672225492L;
    private List<String> fields;

    public static class Context extends BaseAggregator.Context {
        Map<String, Long> invalidDataCount = new HashMap<>();
        long count = 0L;
        Tuple result;
    }

    public DupFieldsAggregator(Fields fieldDeclaration, List<String> fields) {
        super(fieldDeclaration);
        this.fields = fields;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        String invalidVal = "";
        for (int i = 0; i < fields.size(); i++) {
            String fieldValue = String.valueOf(arguments.getObject(fields.get(i)));
            invalidVal += "_" + fields.get(i) + "_" + fieldValue;
        }
        if (context.invalidDataCount.containsKey(invalidVal)) {
            Long countVal = context.invalidDataCount.get(invalidVal);
            countVal++;
            context.invalidDataCount.put(invalidVal, countVal);
        } else {
            context.invalidDataCount.put(invalidVal, 1L);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        String key = "Dup";
        for (int i = 0; i < fields.size(); i++) {
            key += fields.get(i);
        }
        Iterator<Entry<String, Long>> iterate = context.invalidDataCount.entrySet().iterator();
        while (iterate.hasNext()) {
            Map.Entry<String, Long> pair = (Map.Entry<String, Long>) iterate.next();
            if ((Long) pair.getValue() > 1L) {
                context.result = Tuple.size(getFieldDeclaration().size());
                context.result.set(0, key);
                context.result.set(1, pair.getKey());
            } else {
                return null;
            }
        }
        return context.result;
    }
}
