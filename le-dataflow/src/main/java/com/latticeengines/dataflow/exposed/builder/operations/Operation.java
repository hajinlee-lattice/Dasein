package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public abstract class Operation {
    protected CascadingDataFlowBuilder builder;
    protected List<DataFlowBuilder.FieldMetadata> metadata;
    protected Pipe pipe;

    public Operation(CascadingDataFlowBuilder builder) {
        this.builder = builder;
    }

    public List<DataFlowBuilder.FieldMetadata> getOutputMetadata() {
        return metadata;
    }

    public Pipe getOutputPipe() {
        return pipe;
    }

    protected Pipe getPipe(String identifier) {
        return getPipeAndMetadata(identifier).getKey();
    }

    protected List<DataFlowBuilder.FieldMetadata> getMetadata(String identifier) {
        return getPipeAndMetadata(identifier).getValue();
    }

    private AbstractMap.SimpleEntry<Pipe, List<DataFlowBuilder.FieldMetadata>> getPipeAndMetadata(String identifier) {
        return builder.getPipeAndMetadata(identifier);
    }

    protected Fields getFiels(String identfier) {
        List<String> fieldNames = new ArrayList<>();
        for (DataFlowBuilder.FieldMetadata metadata: getMetadata(identfier)) {
            fieldNames.add(metadata.getFieldName());
        }
        return new Fields(fieldNames.toArray(new String[fieldNames.size()]));
    }
}
