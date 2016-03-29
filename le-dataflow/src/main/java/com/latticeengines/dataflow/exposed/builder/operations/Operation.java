package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;

public abstract class Operation {
    public static class Input {
        public Input(Pipe pipe, List<FieldMetadata> metadata) {
            this.pipe = pipe;
            this.metadata = metadata;
        }

        public Pipe pipe;
        public List<FieldMetadata> metadata;
    }

    protected List<FieldMetadata> metadata;
    protected Pipe pipe;

    public List<FieldMetadata> getOutputMetadata() {
        return metadata;
    }

    public Pipe getOutputPipe() {
        return pipe;
    }

    protected Fields getFields(Input input) {
        List<String> fieldNames = new ArrayList<>();
        for (FieldMetadata metadata : input.metadata) {
            fieldNames.add(metadata.getFieldName());
        }
        return new Fields(fieldNames.toArray(new String[fieldNames.size()]));
    }
}
