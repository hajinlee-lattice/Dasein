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
public class CheckFieldNotEmptyFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1112050510297918338L;
    private String checkField;
    private List<String> keyField;

    private static final Log log = LogFactory.getLog(CheckFieldNotEmptyFunction.class);

    public CheckFieldNotEmptyFunction(String checkNullField, List<String> keyField) {
        super(generateFieldDeclaration());
        this.checkField = checkNullField;
        this.keyField = keyField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        try {
            Object checkEmptyVal = arguments.getObject(checkField.toString());
            Object keyFieldVal = "";
            for (int i = 0; i < keyField.size(); i++) {
                if (i == 0) {
                    keyFieldVal = arguments.getObject(keyField.get(i));
                } else {
                    keyFieldVal = keyFieldVal + "," + arguments.getObject(keyField.get(i));
                }
            }
            if (checkEmptyVal instanceof String) {
                if (StringUtils.isEmpty((String) checkEmptyVal)) {
                    result = setTupleValues(result, keyFieldVal, checkField, checkEmptyVal);
                    functionCall.getOutputCollector().add(result);
                }
            } else {
                if (checkEmptyVal == null || checkEmptyVal == "") {
                    result = setTupleValues(result, keyFieldVal, checkField, checkEmptyVal);
                    functionCall.getOutputCollector().add(result);
                }
            }
        } catch (Exception e) {
            log.info("Exception raised due to : " + e);
        }
    }

    private static Tuple setTupleValues(Tuple result, Object keyFieldValue, Object checkField,
            Object checkEmptyField) {
        // check code
        result.set(0, CheckCode.EmptyField.name());
        // row id
        result.set(1, keyFieldValue);
        // check field
        result.set(3, checkField);
        // check value
        result.set(4, checkEmptyField);
        // check message
        result.set(5, CheckCode.EmptyField.getMessage(checkField));
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
