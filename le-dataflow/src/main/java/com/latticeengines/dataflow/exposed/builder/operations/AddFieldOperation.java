package com.latticeengines.dataflow.exposed.builder.operations;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.AddFieldStrategy;
import com.latticeengines.dataflow.runtime.cascading.AddFieldFunction;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class AddFieldOperation extends Operation {


    public AddFieldOperation(String prior, AddFieldStrategy strategy, CascadingDataFlowBuilder builder) {
        // use all fields as arguments
        super(builder);
        Pipe p = getPipe(prior);
        this.metadata = getMetadata(prior);

        Fields priorFields = getFiels(prior);

        DataFlowBuilder.FieldMetadata newFieldMetadata = strategy.newField();
        Fields newField = new Fields(newFieldMetadata.getFieldName());

        Fields fieldsToApply;
        Fields outputSelector;
        if (priorFields.contains(newField)) {
            // swap out a field
            fieldsToApply = priorFields.select(newField);
            outputSelector = Fields.SWAP;
        } else {
            fieldsToApply = Fields.ALL;
            outputSelector = Fields.ALL;
        }

        this.pipe = new Each(p, fieldsToApply, new AddFieldFunction(strategy, newField), outputSelector);
        if (outputSelector == Fields.ALL) { this.metadata.add(newFieldMetadata); }
    }

}
