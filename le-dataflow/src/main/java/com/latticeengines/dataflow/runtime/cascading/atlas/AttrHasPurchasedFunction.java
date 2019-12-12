package com.latticeengines.dataflow.runtime.cascading.atlas;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AttrHasPurchasedFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7617381531527543566L;

    private boolean isDefaultNull;

    public AttrHasPurchasedFunction(String targetField, boolean isDefaultNull) {
        super(new Fields(targetField));
        this.isDefaultNull = isDefaultNull;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (isDefaultNull) {
            Object totalAmountObj = arguments.getObject(InterfaceName.TotalAmount.name());
            Boolean hasPurchased = null;

            if ((totalAmountObj != null)) {
                hasPurchased = ((Double) totalAmountObj) > 0.0;
            }

            functionCall.getOutputCollector().add(new Tuple(hasPurchased));
        } else {
            double totalAmount = arguments.getDouble(InterfaceName.TotalAmount.name()); // treat
                                                                                        // null
                                                                                        // as
                                                                                        // 0
            boolean hasPurchased = totalAmount > 0.0;
            functionCall.getOutputCollector().add(new Tuple(hasPurchased));
        }

    }
}
