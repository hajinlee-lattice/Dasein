package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import java.util.List;

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

    private List<String> checkField;
    private Boolean withStatusFlag;

    public static class Context extends BaseAggregator.Context {
        String value = null;
        String code = null;
        long count = 0;
    }

    public DuplicatedValueCheckAggregator(List<String> keyField, Boolean withStatusFlag) {
        super(generateFieldDeclaration());
        this.checkField = keyField;
        this.withStatusFlag = withStatusFlag;
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
        boolean returnVal = true;
        for (int i = 0; i < checkField.size(); i++) {
            returnVal = returnVal && (group.getObject(checkField.get(i)) == null);
        }
        return returnVal;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        boolean returnVal = true;
        Object grpObjVal = "";
        for(int i = 0; i < checkField.size(); i++) {
            if (i == 0 && (grpObjVal != "")) {
                grpObjVal = String.valueOf(group.getObject(checkField.get(i)));
            } else {
                if (grpObjVal != "") {
                    grpObjVal = grpObjVal + "," + String.valueOf(group.getObject(checkField.get(i)));
                }
                if (grpObjVal == "") {
                    grpObjVal = String.valueOf(group.getObject(checkField.get(i)));
                }
            }
            returnVal = returnVal && (String.valueOf(group.getObject(checkField.get(i))) == null);
        }
        if (returnVal) {
            return context;
        }
        if (grpObjVal instanceof Utf8) {
            context.value = grpObjVal.toString();
        } else if (grpObjVal instanceof String) {
            context.value = (String) grpObjVal;
        }
        if (withStatusFlag != null) {
            if (withStatusFlag) {
                context.code = CheckCode.DuplicatedValuesWithStatus.name();
            } else {
                context.code = CheckCode.DuplicatedValue.name();
            }
        } else {
            context.code = CheckCode.DuplicatedValue.name();
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
            result.set(0, context.code);
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
