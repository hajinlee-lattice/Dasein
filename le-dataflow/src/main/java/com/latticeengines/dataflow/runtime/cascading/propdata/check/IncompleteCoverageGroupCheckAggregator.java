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

public class IncompleteCoverageGroupCheckAggregator extends BaseAggregator<IncompleteCoverageGroupCheckAggregator.Context>
        implements Aggregator<IncompleteCoverageGroupCheckAggregator.Context> {

    private static final long serialVersionUID = -63265549792881813L;

    private String checkField;
    private List<String> coverageFieldList;

    public IncompleteCoverageGroupCheckAggregator(String checkField, List<String> coverageFieldList) {
        super(generateFieldDeclaration());
        this.checkField = checkField;
        this.coverageFieldList = coverageFieldList;
    }

    public static class Context extends BaseAggregator.Context {
        HashSet<String> set = new HashSet<String>();
        int count = 0;
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
        String checkFieldVal = (String) arguments.getObject(checkField);
        if (checkFieldVal != null)
            context.set.add(checkFieldVal);
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        String missingList = "";
        Tuple result = null;
        if (!context.set.isEmpty()) {
            for (String obj : coverageFieldList) {
                System.out.println("coverageFieldList : " + obj);
                if (!((context.set).contains(obj))) {
                    System.out.println("Entered inside : obj : " + obj);
                    if (result == null) {
                        result = Tuple.size(getFieldDeclaration().size());
                        result.set(0, CheckCode.OutOfCoverageGroup.name());
                        result.set(3, checkField);
                    }
                    if (missingList.isEmpty()) {
                        missingList += obj;
                    } else {
                        missingList += ", " + obj;
                    }

                }
            }
            if (!missingList.isEmpty()) {
                result.set(4, missingList);
                result.set(5, CheckCode.OutOfCoverageGroup.getMessage(checkField, missingList));
                return result;
            }
        }
        return null;
    }

}
