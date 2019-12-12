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
public class AttrMarginFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = -6183109795216528748L;

    public AttrMarginFunction(InterfaceName field) {
        super(new Fields(field.name()));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object totalAmountObj = arguments.getObject(InterfaceName.TotalAmount.name());
        Object totalCostObj = arguments.getObject(InterfaceName.TotalCost.name());
        Double margin = null;

        if (totalAmountObj != null && totalCostObj != null && ((Double) totalAmountObj > 0.0)) {
            Double totalAmount = (Double) totalAmountObj;
            Double totalCost = (Double) totalCostObj;
            margin = (totalAmount - totalCost) / totalAmount;
        }

        functionCall.getOutputCollector().add(new Tuple(margin));
    }
}
