package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
public class IncompleteCoverageRowCheckFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2409292458923849875L;
    private static final Log log = LogFactory.getLog(IncompleteCoverageRowCheckFunction.class);
    private Object checkField;
    private List<Object> coverageFields;
    private Object keyField;

    public IncompleteCoverageRowCheckFunction(Object checkField, List<Object> coverageFields, Object keyField) {
        super(generateFieldDeclaration());
        this.checkField = checkField;
        this.coverageFields = coverageFields;
        this.keyField = keyField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        try {
            Object checkCoverageField = arguments.getObject(checkField.toString());
            Object keyFieldVal = arguments.getObject(keyField.toString());
            if (checkCoverageField instanceof String) {
                if (StringUtils.isEmpty(checkCoverageField.toString())
                        || !coverageFields.contains(checkCoverageField)) {
                    functionCall.getOutputCollector()
                            .add(setTupleVal(result, keyFieldVal, checkField, checkCoverageField));
                }
            } else {
                if (checkCoverageField == "" || checkCoverageField == null
                        || !coverageFields.contains(checkCoverageField)) {
                    functionCall.getOutputCollector()
                            .add(setTupleVal(result, keyFieldVal, checkField, checkCoverageField));
                }
            }

        } catch (Exception e) {
            log.info("Exception raised due to : " + e);
        }
    }

    private static Tuple setTupleVal(Tuple result, Object keyFieldVal, Object checkField, Object checkCoverageField) {
        // check code
        result.set(0, CheckCode.OutOfCoverageValForRow.name());
        // row id
        result.set(2, keyFieldVal);
        // check field
        result.set(3, checkField);
        // check value
        result.set(4, checkCoverageField);
        // check message
        result.set(5, CheckCode.OutOfCoverageValForRow.getMessage(keyFieldVal, checkField, checkCoverageField));
        return result;
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

}
