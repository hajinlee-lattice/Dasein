package com.latticeengines.dellebi.process.dailyrefresh.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ScrubOdrDtlFunctionReplace extends BaseOperation implements Function {

    private static final long serialVersionUID = 7269288689250684106L;

    public ScrubOdrDtlFunctionReplace(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String svcTagId = scrub(argument.getString("SVC_TAG_ID"));
        String srcBuId = argument.getString("SRC_BU_ID");
        Tuple result = new Tuple();

        result.add(svcTagId);
        result.add(srcBuId);

        functionCall.getOutputCollector().add(result);
    }

    private String scrub(String s) {
        String returnString = "";
        if (s != null && !s.isEmpty() && s.trim().length() > 12) {
            s.trim();
            returnString = s.substring(0, 12);
        }
        return returnString;
    }
}
