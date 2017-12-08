package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class UnexpectedValueCheckFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 4586043555491363971L;
    private String totalCount;
    private int thresholdCount;

    public UnexpectedValueCheckFunction(String totalCount, int thresholdCount) {
        super(generateFieldDeclaration());
        this.totalCount = totalCount;
        this.thresholdCount = thresholdCount;
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
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        try {
            int totalCountValue = Integer.parseInt(arguments.getObject(totalCount).toString());
            if (totalCountValue > thresholdCount) {
                // check code
                result.set(0, CheckCode.ExceededCount.name());
                // check field
                result.set(3, totalCount);
                // check value
                result.set(4, totalCountValue);
                // check message
                result.set(5, CheckCode.ExceededCount.getMessage(thresholdCount));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
        }
    }

}
