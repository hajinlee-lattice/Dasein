package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.UUID;

import cascading.pipe.Merge;
import cascading.pipe.Pipe;

public class MergeOperation extends Operation {

    public MergeOperation(Input lhs, Input rhs) {
        Pipe lhsPipe = new RenamePipeOperation(lhs, UUID.randomUUID().toString()).pipe;
        Pipe rhsPipe = new RenamePipeOperation(rhs, UUID.randomUUID().toString()).pipe;
        this.pipe = new Merge(lhsPipe, rhsPipe);
        this.metadata = lhs.metadata;
    }

    public MergeOperation(Input[] seeds) {
        Pipe[] pipes = new Pipe[seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            pipes[i] = seeds[i].pipe;
        }
        this.pipe = new Merge(pipes);
        this.metadata = seeds[0].metadata;
    }
}
