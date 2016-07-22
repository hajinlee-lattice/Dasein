package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class DebugOperation extends Operation {
    public DebugOperation(Input prior, String prefix, int printFieldsEvery) {
        Pipe priorPipe = prior.pipe;
        Debug debug = new Debug(prefix, true);
        debug.setPrintFieldsEvery(printFieldsEvery);
        this.pipe = new Each(priorPipe, debug);
        this.metadata = prior.metadata;
    }
}
