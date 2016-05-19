package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.UUID;

import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MergeOperation extends Operation {

    public MergeOperation(Input lhs, Input rhs) {
        Pipe lhsPipe = new RenamePipeOperation(lhs, UUID.randomUUID().toString()).pipe;
        Pipe rhsPipe = new RenamePipeOperation(rhs, UUID.randomUUID().toString()).pipe;

        Pipe merged = new Merge(lhsPipe, rhsPipe);
        // NOTE: This works around what appears to be a cascading bug in merge
        // "Union of steps have x fewer elements than parent assembly"
        this.pipe = new GroupBy(merged, Fields.NONE);
        this.metadata = lhs.metadata;
    }

    public MergeOperation(Input[] seeds) {
        Pipe[] pipes = new Pipe[seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            pipes[i] = seeds[i].pipe;
        }
        Pipe merged = new Merge(pipes);
        this.pipe = new GroupBy(merged, Fields.NONE);
        this.metadata = seeds[0].metadata;
    }
}
