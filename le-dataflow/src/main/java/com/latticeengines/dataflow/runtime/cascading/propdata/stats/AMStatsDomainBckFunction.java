package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMStatsDomainBckFunction extends BaseOperation<Map> //
        implements Function<Map> {
    private static final long serialVersionUID = -4039806083023012431L;

    private String domainField;
    private String domainBckField;

    private int domainFieldLoc;
    private int domainBckFieldLoc;

    public AMStatsDomainBckFunction(Params params) {
        super(params.outputFieldsDeclaration);

        domainField = params.domainField;
        domainBckField = params.domainBckField;

        domainFieldLoc = params.outputFieldsDeclaration.getPos(domainField);
        domainBckFieldLoc = params.outputFieldsDeclaration.getPos(domainBckField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Map> functionCall) {
        TupleEntry entry = functionCall.getArguments();

        Tuple result = entry.getTupleCopy();

        String domain = (String) entry.getObject(domainFieldLoc);

        result.set(domainBckFieldLoc, domain);

        functionCall.getOutputCollector().add(result);
    }

    public static class Params {
        Fields outputFieldsDeclaration;
        String domainField;
        String domainBckField;

        public Params(Fields outputFieldsDeclaration, //
                String domainField, //
                String domainBckField) {
            this.outputFieldsDeclaration = outputFieldsDeclaration;
            this.domainField = domainField;
            this.domainBckField = domainBckField;
        }
    }
}
