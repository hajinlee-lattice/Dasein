package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AttrHasPurchasedFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7617381531527543566L;

    public AttrHasPurchasedFunction(String targetField) {
        super(new Fields(targetField));
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
