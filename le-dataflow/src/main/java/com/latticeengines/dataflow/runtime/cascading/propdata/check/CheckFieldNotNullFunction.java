package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import org.apache.commons.lang3.StringUtils;

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
public class CheckFieldNotNullFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1112050510297918338L;
    private String checkField;
    private String keyField;

    public CheckFieldNotNullFunction(String checkNullField, String keyField) {
        super(generateFieldDeclaration());
        this.checkField = checkNullField;
        this.keyField = keyField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        try {
            String checkNullField = (String) arguments.getObject(checkField);
            Integer keyFieldValue = (Integer) arguments.getObject(keyField);
            if (StringUtils.isEmpty(checkNullField)) {
                // check code
                result.set(0, CheckCode.EmptyOrNullField.name());
                // row id
                result.set(1, keyFieldValue);
                // check field
                result.set(3, checkField);
                // check value
                result.set(4, checkNullField);
                // check message
                result.set(5, CheckCode.EmptyOrNullField.getMessage(checkField));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
        }
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
