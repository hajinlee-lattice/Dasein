package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.List;

import cascading.tuple.TupleEntry;

public interface DepivotStrategy extends Serializable {

    List<List<Object>> depivot(TupleEntry arguments);

}
