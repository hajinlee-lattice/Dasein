package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.pipe.Checkpoint;

public class CheckPointOperation extends Operation {

    public CheckPointOperation(Input prior) {
        this.pipe = new Checkpoint(prior.pipe);
        this.metadata = prior.metadata;
    }

}
