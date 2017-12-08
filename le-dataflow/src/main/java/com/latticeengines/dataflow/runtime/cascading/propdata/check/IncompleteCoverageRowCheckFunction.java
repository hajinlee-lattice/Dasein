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
    private String checkField;
    private List<String> coverageFields;
    private String keyField;

    public IncompleteCoverageRowCheckFunction(String checkField, List<String> coverageFields, String keyField) {
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
            String checkCoverageField = (String) arguments.getObject(checkField);
            int keyFieldVal = Integer.parseInt(arguments.getObject(keyField).toString());
            if (StringUtils.isEmpty(checkCoverageField) || !coverageFields.contains(checkCoverageField)) {
                // check code
                result.set(0, CheckCode.OutOfCoverageRow.name());
                // row id
                result.set(2, keyFieldVal);
                // check field
                result.set(3, checkField);
                // check value
                result.set(4, checkCoverageField);
                // check message
                result.set(5, CheckCode.OutOfCoverageRow.getMessage(keyFieldVal, checkField, checkCoverageField));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
            log.info("Exception raised due to : " + e);
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
