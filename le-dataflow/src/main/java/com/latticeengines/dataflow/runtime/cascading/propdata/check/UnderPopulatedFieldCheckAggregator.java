package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import org.apache.avro.util.Utf8;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UnderPopulatedFieldCheckAggregator extends BaseAggregator<UnderPopulatedFieldCheckAggregator.Context>
        implements Aggregator<UnderPopulatedFieldCheckAggregator.Context> {

    private static final long serialVersionUID = -5205919005219754992L;
    private String checkField;
    private int threshold;
    private int totalCount;

    public UnderPopulatedFieldCheckAggregator(String checkField, int threshold, int totalCount) {
        super(generateFieldDeclaration());
        this.checkField = checkField;
        this.threshold = threshold;
        this.totalCount = totalCount;
    }

    private static Fields generateFieldDeclaration() {
        return new Fields( //
                DataCloudConstants.CHK_ATTR_CHK_CODE, //
                DataCloudConstants.CHK_ATTR_ROW_ID, //
                DataCloudConstants.CHK_ATTR_GROUP_ID, //
                DataCloudConstants.CHK_ATTR_CHK_FIELD, //
                DataCloudConstants.CHK_ATTR_CHK_VALUE, //
                DataCloudConstants.CHK_ATTR_CHK_MSG);
    }

    public static class Context extends BaseAggregator.Context {
        String value = null;
        long count = 0;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(checkField);
        return grpObj == null;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        Object grpObjVal = group.getObject(checkField);
        if (grpObjVal == null) {
            return context;
        }
        if (grpObjVal instanceof Utf8) {
            context.value = grpObjVal.toString();
            context.count += 1;
        } else if (grpObjVal instanceof String) {
            context.value = (String) grpObjVal;
            context.count += 1;
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
        double fieldPopulationPercent = (context.count / (totalCount * 1.0)) * 100;
        if (fieldPopulationPercent < threshold) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            // check code
            result.set(0, CheckCode.UnderPopulatedField.name());
            // group id
            result.set(2, context.value);
            // check field
            result.set(3, checkField);
            // check value
            result.set(4, String.valueOf(context.count));
            // check message
            result.set(5, CheckCode.UnderPopulatedField.getMessage(checkField, String.valueOf(context.count),
                    String.valueOf(context.count)));
            return result;
        } else {
            return null;
        }
    }

}
