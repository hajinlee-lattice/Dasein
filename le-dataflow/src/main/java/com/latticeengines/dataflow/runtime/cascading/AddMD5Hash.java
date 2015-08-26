package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;

import org.apache.commons.codec.digest.DigestUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AddMD5Hash extends BaseOperation implements Function {

    private static final long serialVersionUID = -9005009537092431868L;

    @ConstructorProperties({ "fieldDeclaration" })
    public AddMD5Hash(Fields fieldDeclaration) {
        super(0, fieldDeclaration);
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String data = "";
        TupleEntry entry = functionCall.getArguments();
        for (int i = 0; i < entry.getFields().size(); i++) {
            if (i > 0) {
                data += "|"; 
            }
            Object tupleValue = entry.getTuple().getObject(i);
            
            if (tupleValue == null) {
                tupleValue = "<null>";
            }
            data += tupleValue.toString();
        }
        
        
        functionCall.getOutputCollector().add(new Tuple(DigestUtils.md5Hex(data)));
    }

}
