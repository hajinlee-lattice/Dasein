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
public class CheckFieldPopulationThresholdFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1798670131716639327L;
    private String totalCount;
    private double thresholdCount;
    private String checkField;
    private String populatedCount;

    public CheckFieldPopulationThresholdFunction(String totalCount, String populatedCount, double thresholdCount,
            String checkField) {
        super(generateFieldDeclaration());
        this.totalCount = totalCount;
        this.thresholdCount = thresholdCount;
        this.checkField = checkField;
        this.populatedCount = populatedCount;
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
            int populatedCountVal = Integer.parseInt(arguments.getObject(populatedCount).toString());
            double fieldPopulation = (populatedCountVal / (totalCountValue * 1.0)) * 100;
            if (fieldPopulation < thresholdCount) {
                double diff = thresholdCount - fieldPopulation;
                // check code
                result.set(0, CheckCode.UnderPopulatedField.name());
                // check field
                result.set(3, checkField);
                // check value
                result.set(4, String.format("%.2f", fieldPopulation));
                // check message
                result.set(5, CheckCode.UnderPopulatedField.getMessage(checkField,
                        String.format("%.2f", fieldPopulation), String.format("%.2f", diff)));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
        }
    }

}
