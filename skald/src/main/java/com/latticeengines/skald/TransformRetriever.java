package com.latticeengines.skald;

import org.springframework.stereotype.Service;

import com.latticeengines.skald.model.FieldType;

@Service
public class TransformRetriever {
    public PythonTransform getTransform(String name, FieldType type) {
        // TODO Implement a caching strategy for these.

        // TODO Actually retrieve these from somewhere.
        return new PythonTransform("def transform(*args):\n    return 10\n\n", FieldType.Float);
    }
}
