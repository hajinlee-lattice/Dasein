package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionUtil.ExpandedTuple;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMStatsDimensionAggregator extends BaseAggregator<AMStatsDimensionAggregator.Context>
        implements Aggregator<AMStatsDimensionAggregator.Context> {
    private static final long serialVersionUID = 4217950767704131475L;

    private AMStatsDimensionUtil dimensionUtil;

    public static class Context extends BaseAggregator.Context {
        ExpandedTuple mergedTuple = null;
    }

    public AMStatsDimensionAggregator(Fields fieldDeclaration) {
        super(fieldDeclaration);
        dimensionUtil = new AMStatsDimensionUtil();
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        Tuple tuple = arguments.getTuple();

        if (context.mergedTuple == null) {
            context.mergedTuple = new ExpandedTuple(tuple);
        } else {
            context.mergedTuple = //
                    dimensionUtil.merge(context.mergedTuple, //
                            new ExpandedTuple(tuple), //
                            tuple.size());
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = context.mergedTuple.generateTuple();
        context.mergedTuple = null;
        return result;
    }
}
