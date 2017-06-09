package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ProfileSampleAggregator extends BaseAggregator<ProfileSampleAggregator.Context>
        implements Aggregator<ProfileSampleAggregator.Context> {

    private static final long serialVersionUID = -4518166131558866707L;

    private List<String> attrs;

    public static class Context extends BaseAggregator.Context {
        Map<String, Object> elems;
        Map<String, Integer> seqs;
    }

    public ProfileSampleAggregator(Fields fieldDeclaration, List<String> attrs) {
        super(fieldDeclaration);
        this.attrs = attrs;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.elems = new HashMap<>();
        context.seqs = new HashMap<>();
        for (String attr : attrs) {
            context.seqs.put(attr, 0);
        }
        return context;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        for (String attr : attrs) {
            Object val = arguments.getObject(attr);
            if (val == null) {
                continue;
            }
            context.seqs.put(attr, context.seqs.get(attr) + 1);
            if (!context.elems.containsKey(attr)) {
                context.elems.put(attr, val);
                continue;
            }
            double prob = 1.0 / context.seqs.get(attr);
            if (Math.random() <= prob) {
                context.elems.put(attr, val);
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (String attr : attrs) {
            int loc = namePositionMap.get(attr);
            result.set(loc, context.elems.get(attr));
        }
        return result;
    }
}
