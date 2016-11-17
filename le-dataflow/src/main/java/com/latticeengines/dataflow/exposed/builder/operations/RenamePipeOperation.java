package com.latticeengines.dataflow.exposed.builder.operations;

import com.google.api.client.util.Lists;

import cascading.pipe.Pipe;

public class RenamePipeOperation extends Operation {

    public RenamePipeOperation(Input prior, String newname) {
        Pipe pipe = new Pipe(newname, prior.pipe);
        this.pipe = pipe;
        this.metadata = Lists.newArrayList(prior.metadata);
    }
}
