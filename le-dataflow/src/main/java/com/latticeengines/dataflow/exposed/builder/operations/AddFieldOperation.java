package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.dataflow.exposed.builder.strategy.AddFieldStrategy;
import com.latticeengines.dataflow.runtime.cascading.AddFieldFunction;

public class AddFieldOperation extends Operation {

    public AddFieldOperation(Input prior, AddFieldStrategy strategy) {
        // use all fields as arguments
        Pipe p = prior.pipe;
        this.metadata = prior.metadata;

        Fields priorFields = getFields(prior);

        FieldMetadata newFieldMetadata = strategy.newField();
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
        if (outputSelector == Fields.ALL) {
            this.metadata.add(newFieldMetadata);
        }
    }
}
