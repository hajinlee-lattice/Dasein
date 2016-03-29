package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;

public interface AddFieldStrategy extends Serializable {

    Object compute(TupleEntry arguments);

    FieldMetadata newField();

    Fields argumentSelector();

}
