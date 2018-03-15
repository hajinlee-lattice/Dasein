package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class UpdatePrimDomAlexaRankFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -462152396830896380L;
    private Map<String, Integer> namePositionMap;

    private String secDomainField;
    private String primDomainField;
    private String alexaRankField;
    private String ldcDomainField;
    private String primDomAlexaRankField;

    public UpdatePrimDomAlexaRankFunction(Fields fieldDeclaration, String secDomainField, String primDomainField,
            String alexaRankField,
            String ldcDomainField, String primDomAlexaRankField) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.secDomainField = secDomainField;
        this.primDomainField = primDomainField;
        this.alexaRankField = alexaRankField;
        this.ldcDomainField = ldcDomainField;
        this.primDomAlexaRankField = primDomAlexaRankField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        if (arguments.getObject(secDomainField) != null) {
            result.set(this.namePositionMap.get(ldcDomainField), arguments.getObject(primDomainField));
            result.set(this.namePositionMap.get(alexaRankField), arguments.getObject(primDomAlexaRankField));
        }
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
