package com.latticeengines.dataflow.exposed.builder.operations;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MergeOperation extends Operation {

    public MergeOperation(String lhs, String rhs, CascadingDataFlowBuilder builder) {
        super(builder);
        Pipe lhsPipe = getPipe(lhs);
        Pipe rhsPipe = getPipe(rhs);

        Pipe merged = new Merge(lhsPipe, rhsPipe);
        // NOTE: This works around what appears to be a cascading bug in merge
        // "Union of steps have x fewer elements than parent assembly"
        GroupBy groupby = new GroupBy(merged, Fields.NONE);
        this.pipe = groupby;
        this.metadata = getMetadata(lhs);
    }
}
