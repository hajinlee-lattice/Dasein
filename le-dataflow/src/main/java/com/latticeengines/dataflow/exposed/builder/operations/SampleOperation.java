package com.latticeengines.dataflow.exposed.builder.operations;

import cascading.operation.filter.Sample;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class SampleOperation extends Operation {
    public SampleOperation(Input prior, float fraction) {
        Pipe priorPipe = prior.pipe;
        this.pipe = new Each(priorPipe, new Sample(fraction));
        this.metadata = prior.metadata;
    }
}
