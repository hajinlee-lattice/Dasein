package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;

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
    public List<List<Object>> depivot(TupleEntry arguments) {
        List<List<Object>> result = new ArrayList<>();
        for (List<String> sourceTuple : sourceFieldTuples) {
            List<Object> valueTuple = new ArrayList<>();
            for (String field : sourceTuple) {
                Object value = arguments.getObject(field);
                if (value == null || (value instanceof String && StringUtils.isEmpty((String) value))) {
                    // no need to handle null depivot field, skip
                    continue;
                }
                valueTuple.add(value);
            }

            if (valueTuple.size() == 0) {
                // if there is no non-null depivot field then simply skip
                continue;
            }

            result.add(valueTuple);
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
