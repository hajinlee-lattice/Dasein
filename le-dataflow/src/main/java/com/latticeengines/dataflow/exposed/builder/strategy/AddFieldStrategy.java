package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public interface AddFieldStrategy extends Serializable {

    Object compute(TupleEntry arguments);

    DataFlowBuilder.FieldMetadata newField();

    Fields argumentSelector();

}
