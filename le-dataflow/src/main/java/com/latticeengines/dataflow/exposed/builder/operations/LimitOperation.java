package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.operation.filter.Limit;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

public class LimitOperation extends Operation {
    public LimitOperation(String prior, int count, CascadingDataFlowBuilder builder) {
        super(builder);

        if (!builder.enforceGlobalOrdering()) {
            throw new RuntimeException(
                    "Builder must enforce global ordering in order to perform a deterministic limit operation");
        }

        Pipe priorPipe = getPipe(prior);
        this.pipe = new Each(priorPipe, new Limit(count));
        this.metadata = getMetadata(prior);
    }
}
