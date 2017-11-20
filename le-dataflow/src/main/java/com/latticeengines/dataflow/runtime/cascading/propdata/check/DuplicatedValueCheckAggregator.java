package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import org.apache.avro.util.Utf8;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DuplicatedValueCheckAggregator extends BaseAggregator<DuplicatedValueCheckAggregator.Context>
        implements Aggregator<DuplicatedValueCheckAggregator.Context> {

    private static final long serialVersionUID = 7863554582861435361L;

    private String checkField;

    public static class Context extends BaseAggregator.Context {
        String value = null;
        long count = 0;
    }

    public DuplicatedValueCheckAggregator(String keyField) {
        super(generateFieldDeclaration());
        this.checkField = keyField;
    }

    private static Fields generateFieldDeclaration() {
        return new Fields( //
                DataCloudConstants.CHK_ATTR_CHK_CODE, //
                DataCloudConstants.CHK_ATTR_ROW_ID, //
                DataCloudConstants.CHK_ATTR_GROUP_ID, //
                DataCloudConstants.CHK_ATTR_CHK_FIELD, //
                DataCloudConstants.CHK_ATTR_CHK_VALUE, //
                DataCloudConstants.CHK_ATTR_CHK_MSG
        );
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(checkField);
        return grpObj == null;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        Object grpObj = group.getObject(checkField);
        if (grpObj == null) {
            return context;
        }
        if (grpObj instanceof Utf8) {
            context.value = grpObj.toString();
        } else if (grpObj instanceof String) {
            context.value = (String) grpObj;
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
        if (context.count > 1) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            // check code
            result.set(0, CheckCode.DuplicatedValue.name());
            // group id
            result.set(2, context.value);
            // check field
            result.set(3, checkField);
            // check value
            result.set(4, String.valueOf(context.count));
            // check message
            result.set(5, CheckCode.DuplicatedValue.getMessage(context.value, checkField));
            return result;
        } else {
            return null;
        }
    }

    protected Tuple dummyTuple(Context context) {
        return null;
    }
}
