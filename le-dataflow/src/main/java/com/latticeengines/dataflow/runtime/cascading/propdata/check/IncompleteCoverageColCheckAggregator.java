package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import java.util.HashSet;
import java.util.List;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class IncompleteCoverageColCheckAggregator extends BaseAggregator<IncompleteCoverageColCheckAggregator.Context>
        implements Aggregator<IncompleteCoverageColCheckAggregator.Context> {

    private static final long serialVersionUID = -63265549792881813L;

    private Object checkField;
    private List<Object> coverageFieldList;

    public IncompleteCoverageColCheckAggregator(Object checkField, List<Object> coverageFieldList) {
        super(generateFieldDeclaration());
        this.checkField = checkField;
        this.coverageFieldList = coverageFieldList;
    }

    public static class Context extends BaseAggregator.Context {
        HashSet<Object> set = new HashSet<Object>();
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
        String checkFieldVal = (String) arguments.getObject(checkField.toString());
        if (checkFieldVal != null)
            context.set.add(checkFieldVal);
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        StringBuilder missingList = new StringBuilder("");
        Tuple result = Tuple.size(getFieldDeclaration().size());
        if (!context.set.isEmpty()) {
            for (Object obj : coverageFieldList) {
                if (!((context.set).contains(obj))) {
                    result = Tuple.size(getFieldDeclaration().size());
                    result.set(0, CheckCode.OutOfCoverageValForCol.name());
                    result.set(3, checkField);
                    if (missingList.length() == 0) {
                        missingList.append(obj);
                    } else {
                        missingList.append("," + obj);
                    }

                }
            }
            if (missingList.length() != 0) {
                result.set(4, missingList);
                result.set(5, CheckCode.OutOfCoverageValForCol.getMessage(checkField, missingList));
                return result;
            }
        }
        return null;
    }

}
