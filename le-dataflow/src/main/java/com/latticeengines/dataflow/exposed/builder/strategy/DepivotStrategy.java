package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.List;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public interface DepivotStrategy extends Serializable {

    Iterable<Tuple> depivot(TupleEntry arguments);

}
