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
        this.pipe = new GroupBy(merged, Fields.NONE);
        this.metadata = getMetadata(lhs);
    }

    public MergeOperation(String[] seeds, CascadingDataFlowBuilder builder) {
        super(builder);
        Pipe[] pipes = new Pipe[seeds.length];
        for (int i = 0; i <  seeds.length; i++) {
            pipes[i] = getPipe(seeds[i]);
        }
        Pipe merged = new Merge(pipes);
        this.pipe = new GroupBy(merged, Fields.NONE);
        this.metadata = getMetadata(seeds[0]);
    }
}
