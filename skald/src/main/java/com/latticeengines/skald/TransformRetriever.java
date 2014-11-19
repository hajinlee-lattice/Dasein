package com.latticeengines.skald;

import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEvaluator;
import com.latticeengines.skald.model.FieldType;

@Service
public class TransformRetriever {
    public JythonTransform getTransform(String name, FieldType type) {
        // TODO Implement a caching strategy for these.

        // TODO Actually retrieve these from somewhere.
        JythonEvaluator evaluator = new JythonEvaluator(
                "def transform(args, record):\n    return sum(record.values())\n\n");
        return new JythonTransform(evaluator, type);
    }
}
