package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AmManChecksAggregator extends BaseAggregator<AmManChecksAggregator.Context>
        implements Aggregator<AmManChecksAggregator.Context> {

    private static final long serialVersionUID = 8197555095806683562L;
    private String checkField;
    private String checkCode;
    public static class Context extends BaseAggregator.Context {
        String value = null;
        String checkField = null;
        String checkCode = null;
        long count = 0;
    }

    public AmManChecksAggregator(String checkCode, String checkField) {
        super(new Fields(DataCloudConstants.CHK_ATTR_CHK_MSG));
        this.checkField = checkField;
        this.checkCode = checkCode;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj1 = group.getObject(checkField);
        Object grpObj2 = group.getObject(checkCode);
        return grpObj1 == null && grpObj2 == null;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        Object grpObj1 = group.getObject(checkField);
        Object grpObj2 = group.getObject(checkCode);
        if (grpObj1 == null && grpObj2 == null) {
            return context;
        }
        if (grpObj1 instanceof Utf8 && grpObj2 instanceof Utf8) {
            context.checkField = grpObj1.toString();
            context.checkCode = grpObj2.toString();
        } else if (grpObj1 instanceof String && grpObj2 instanceof String) {
            context.checkField = (String) grpObj1;
            context.checkCode = (String) grpObj2;
        }
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        context.count += 1;
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(1);
        String aggregatedMsg = "";
        if (context.count > 0) {
            aggregatedMsg = String.valueOf(context.count) + " Rows/groups violates " + context.checkCode
                    + " check on fields " + context.checkField;
            // check message
            result.set(0, aggregatedMsg);
            return result;
        }
        return null;
    }
}
