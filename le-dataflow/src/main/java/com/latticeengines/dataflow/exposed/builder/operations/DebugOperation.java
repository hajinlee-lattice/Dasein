package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class DebugOperation extends Operation {
    public DebugOperation(Input prior, int printFieldsEvery) {
        Pipe priorPipe = prior.pipe;
        Debug debug = new Debug(true);
        debug.setPrintFieldsEvery(printFieldsEvery);
        this.pipe = new Each(priorPipe, debug);
        this.metadata = prior.metadata;
    }
}
