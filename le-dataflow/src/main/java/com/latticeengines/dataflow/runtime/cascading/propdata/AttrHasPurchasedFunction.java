package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class AttrHasPurchasedFunction extends BaseOperation implements Function {
    public AttrHasPurchasedFunction(InterfaceName field) {
        super(new Fields(field.name()));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object totalAmountObj = arguments.getObject(InterfaceName.TotalAmount.name());
        Boolean hasPurchased = null;

        if ((totalAmountObj != null)) {
            hasPurchased = ((Double) totalAmountObj) > 0.0;
        }

        functionCall.getOutputCollector().add(new Tuple(hasPurchased));
    }
}
