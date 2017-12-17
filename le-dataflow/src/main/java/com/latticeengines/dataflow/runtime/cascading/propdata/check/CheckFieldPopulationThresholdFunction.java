package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private String numOfRecords;
    private double populatePercentThreshold;
    private String checkField;
    private String numOfPopulatedRec;
    private static final Log log = LogFactory.getLog(CheckFieldPopulationThresholdFunction.class);

    public CheckFieldPopulationThresholdFunction(String numOfRecords, String numOfPopulatedRec,
            double populatePercentThreshold,
            String checkField) {
        super(generateFieldDeclaration());
        this.numOfRecords = numOfRecords;
        this.populatePercentThreshold = populatePercentThreshold;
        this.checkField = checkField;
        this.numOfPopulatedRec = numOfPopulatedRec;
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
            long numOfRecordsVal = (Long) (arguments.getObject(numOfRecords));
            long populatedCountVal = (Long) (arguments.getObject(numOfPopulatedRec));
            double fieldPopulationPercent = (populatedCountVal / (numOfRecordsVal * 1.0)) * 100;
            if (fieldPopulationPercent < populatePercentThreshold) {
                double diff = populatePercentThreshold - fieldPopulationPercent;
                // check code
                result.set(0, CheckCode.UnderPopulatedField.name());
                // check field
                result.set(3, checkField);
                // check value
                result.set(4, String.format("%.2f", fieldPopulationPercent));
                // check message
                result.set(5, CheckCode.UnderPopulatedField.getMessage(checkField,
                        String.format("%.2f", fieldPopulationPercent), String.format("%.2f", diff)));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
            log.info("Exception raised due to : " + e);
        }
    }

}
