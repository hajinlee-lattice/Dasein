package com.latticeengines.scoringapi.transform;

import java.util.Map;

import com.latticeengines.common.exposed.jython.JythonEvaluator;

public class JythonTransform {
    public JythonTransform(JythonEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    public <T> T invoke(Map<String, Object> arguments, Map<String, Object> record, Class<T> type) {
        return evaluator.function("transform", type, arguments, record);
    }

    private final JythonEvaluator evaluator;
}
