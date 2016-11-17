package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;

import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public interface AddFieldStrategy extends Serializable {

    Object compute(TupleEntry arguments);

    FieldMetadata newField();

    Fields argumentSelector();

}
