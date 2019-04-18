package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AppendOptLogFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 6037269301787373242L;

    // copyMode = true: copy pre-defined log message from appendFromField and
    // append to LE_OperationLog
    // copyMode = false: append log (message) to LE_OperationLog
    private boolean copyMode;
    private String log;
    private String appendFromField;
    private int optLogIdx;

    /**
     * Only one of log and appendFromField should be populated
     *
     * ATTENTION: LE_OperationLog should already exist in the node,
     * AppendOptLogFunction will not create it. FieldStrategy for
     * AppendOptLogFunction is REPLACE
     *
     * @param fieldDeclaration
     * @param log:
     *            append pre-defined log message to LE_OperationLog field
     * @param appendFromField:
     *            copy log message from appendFromField and append to
     *            LE_OperationLog
     */
    public AppendOptLogFunction(Fields fieldDeclaration, String log, String appendFromField) {
        super(fieldDeclaration);
        Preconditions.checkArgument(validate(log, appendFromField),
                "Only one of log and appendFromField should be populated");
        Map<String, Integer> namePositionMap = getPositionMap(fieldDeclaration);
        this.log = log;
        this.appendFromField = appendFromField;
        optLogIdx = namePositionMap.get(OperationLogUtils.DEFAULT_FIELD_NAME);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = arguments.getTupleCopy();
        appendLog(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void appendLog(Tuple result, TupleEntry arguments) {
        String existingLogs = arguments.getString(OperationLogUtils.DEFAULT_FIELD_NAME);
        String appendLog = copyMode ? arguments.getString(appendFromField) : log;
        result.set(optLogIdx, OperationLogUtils.appendLog(existingLogs, appendLog));
    }

    private boolean validate(String log, String appendFromField) {
        if (StringUtils.isNotBlank(log) && StringUtils.isBlank(appendFromField)) {
            copyMode = false;
            return true;
        }
        if (StringUtils.isBlank(log) && StringUtils.isNotBlank(appendFromField)) {
            copyMode = true;
            return true;
        }
        return false;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
}
