package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SimpleDepivotStragegyImpl implements DepivotStrategy {

    private static final long serialVersionUID = -6067517177276167589L;

    private List<String> targetFields;
    private List<List<String>> sourceFieldTuples;

    public SimpleDepivotStragegyImpl(List<List<String>> sourceFieldTuples, List<String> targetFields) {
        this.sourceFieldTuples = sourceFieldTuples;
        this.targetFields = targetFields;
        validateConfiguration();
    }

    @Override
    public Iterable<Tuple> depivot(TupleEntry arguments) {
        List<Tuple> result = new ArrayList<>();
        for (List<String> sourceTuple : sourceFieldTuples) {
            List<Object> valueTuple = new ArrayList<>();
            boolean hasNotNull = false;
            for (String field : sourceTuple) {
                Object value = arguments.getObject(field);
                if (value instanceof String) {
                    hasNotNull = hasNotNull || StringUtils.isNotEmpty((String) value);
                } else {
                    hasNotNull = hasNotNull || (value != null);
                }
                valueTuple.add(value);
            }

            if (hasNotNull) {
                // skip all null tuple
                Tuple tuple = new Tuple(valueTuple.toArray(new Object[valueTuple.size()]));
                result.add(tuple);
            }
        }
        return result;
    }

    private void validateConfiguration() {
        if (targetFields == null || targetFields.isEmpty()) {
            throw new IllegalArgumentException("Target fields cannot be empty.");
        }

        for (String field : targetFields) {
            if (StringUtils.isEmpty(field)) {
                throw new IllegalArgumentException("Target fields cannot be null.");
            }
        }

        int numTargetFields = targetFields.size();
        for (List<String> tuple : sourceFieldTuples) {
            if (tuple == null || tuple.size() != numTargetFields) {
                int tupleSize = tuple == null ? 0 : tuple.size();
                throw new IllegalArgumentException(
                        String.format("Declared %d target fields, but found %d in one of the source tuples %s",
                                numTargetFields, tupleSize, tuple));
            }
        }
    }

}
