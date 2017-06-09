package com.latticeengines.dataflow.exposed.builder.operations;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class InsertOperation extends Operation {
    public InsertOperation(Input prior, FieldList targetFields, Object... values) {
        Pipe priorPipe = prior.pipe;
        this.pipe = new Each(priorPipe, new Insert(new Fields(targetFields.getFields()), values));
        this.metadata = prior.metadata;
    }
}
