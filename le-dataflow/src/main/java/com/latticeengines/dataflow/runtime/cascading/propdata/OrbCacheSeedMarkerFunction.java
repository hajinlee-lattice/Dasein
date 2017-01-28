package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class OrbCacheSeedMarkerFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2717140611902104160L;

    private List<String> fieldsToCheck;

    public OrbCacheSeedMarkerFunction(String field, List<String> fieldsToCheck) {
        super(new Fields(field));
        this.fieldsToCheck = fieldsToCheck;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple tuple = arguments.getTupleCopy();
        Fields fields = arguments.getFields();
        Tuple result = Tuple.size(1);

        @SuppressWarnings("unchecked")
        Iterator<Object> itr = fields.iterator();
        int pos = 0;

        boolean isSecondaryOrEmail = false;

        while (itr.hasNext() && !isSecondaryOrEmail) {
            String field = (String) itr.next();
            Boolean value = tuple.getBoolean(pos++);
            for (String fieldToCheck : fieldsToCheck) {
                if (field.equals(fieldToCheck) //
                        && value != null //
                        && value == Boolean.TRUE) {
                    isSecondaryOrEmail = true;
                    break;
                }
            }
        }
        result.setBoolean(0, isSecondaryOrEmail);
        functionCall.getOutputCollector().add(result);
    }
}
