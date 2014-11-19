package com.latticeengines.skald;

import java.util.Map;

import com.latticeengines.common.exposed.jython.JythonEvaluator;
import com.latticeengines.skald.model.FieldType;

public class JythonTransform {
    public JythonTransform(JythonEvaluator evaluator, FieldType type) {
        this.evaluator = evaluator;
        this.type = type;
    }

    public Object invoke(Map<String, Object> arguments, Map<String, Object> record) {
        return evaluator.function("transform", type.type(), arguments, record);
    }

    private final JythonEvaluator evaluator;
    private final FieldType type;
}
